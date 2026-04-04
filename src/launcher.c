#include <postgres.h>
#include <fmgr.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <executor/spi.h>
#include <utils/snapmgr.h>
#include <tcop/utility.h>

#include "launcher.h"
#include "job.h"



// bgw signal handler
static volatile sig_atomic_t got_sigterm = false;

static void
launcher_sigterm_handler(SIGNAL_ARGS)
{
    got_sigterm = true;
    SetLatch(MyLatch);
}


/*
    Private function
*/

static bool
is_worker_running(Oid db_oid)
{
    int ret;
    bool exists = false;
    StringInfoData query;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT 1 FROM pg_stat_activity "
        "WHERE application_name = 'job worker' "
        "  AND datid = %u", db_oid);

    ret = SPI_execute(query.data, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
        exists = true;

    return exists;
}


// spawn worker in specific database 
static void
spawn_worker(Oid db_oid)
{
    if(is_worker_running(db_oid))
        return;

    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;

    MemSet(&worker, 0, sizeof(worker));
    strlcpy(worker.bgw_name, "job worker", BGW_MAXLEN);
    strlcpy(worker.bgw_library_name, "auto_job", BGW_MAXLEN);
    strlcpy(worker.bgw_function_name, "job_worker_main", BGW_MAXLEN);

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
                        BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_main_arg = ObjectIdGetDatum(db_oid);

    RegisterDynamicBackgroundWorker(&worker, &handle);
    elog(LOG, "auto_job launcher: spawned job worker for db oid=%u", db_oid);
}


// remove registry entries and terminate workers for database that dropped extension
static void
launcher_cleanup(void)
{
    int ret;
    uint64 dropped_count;
    Oid *dropped_db;
    StringInfoData query;

    // Find db in registry where the db no longer exists or extension was dropped
    ret = SPI_execute(
        "SELECT r.db_oid "
        "FROM public.auto_job_registry r "
        "WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE oid = r.db_oid) "
        "   OR NOT EXISTS ("
        "       SELECT 1 FROM dblink("
        "           format('dbname=%s', r.db_name),"
        "           'SELECT 1 FROM pg_extension WHERE extname = ''auto_job'''"
        "       ) AS t(val int)"
        "   )",
        true, 0);

    if(ret != SPI_OK_SELECT){
        elog(WARNING, "auto_job launcher: failed to check for dropped extensions");
        return;
    }
    
    dropped_count = SPI_processed;
    if (dropped_count == 0)
        return;
    
    // save oid
    dropped_db = palloc(sizeof(Oid) * dropped_count);
    for(uint64 i=0; i < dropped_count; i++){
        bool isnull;
        dropped_db[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
    }

    for(uint64 i=0; i < dropped_count; i++){
        Oid db_oid = dropped_db[i];

        initStringInfo(&query);

        // terminate job worker on that database
        appendStringInfo(&query,
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE application_name = 'job worker' "
            "   AND datid = %u", db_oid);
        SPI_execute(query.data, false, 0);

        // remove from registry
        resetStringInfo(&query);
        appendStringInfo(&query,
            "DELETE FROM public.auto_job_registry WHERE db_oid = %u", db_oid);
        SPI_execute(query.data, false, 0);

        elog(LOG, "auto_job launcher: extension dropped in db oid=%u, cleaned up", db_oid);
    }
}


// spawn worker in database that attached extension 
static void
launcher_spawn_all_workers(void)
{
    int ret;

    ret = SPI_execute(
        "SELECT db_oid FROM public.auto_job_registry",
        true, 0);
    
    if(ret != SPI_OK_SELECT){
        elog(WARNING, "auto_job launcher: fail to query databases");
        return;
    }

    for(uint64 i=0; i < SPI_processed; i++){
        bool isnull;
        Oid db_oid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));

        spawn_worker(db_oid);
    }
}


/*
    Public function
*/

// launcher background worker
void 
launcher_main(Datum main_arg)
{
    pqsignal(SIGTERM, launcher_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    // connect to postgres (default database) to search another database that attached extension
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
    pgstat_report_appname("auto_job launcher");

    elog(LOG, "auto_job launcher started");

    while(!got_sigterm){
        /*  Interrrupt handling logic
                SQL command send SIGUSR1
                        ↓
                WaitLatch active → ResetLatch()
                        ↓
            CHECK_FOR_INTERRUPTS 
    (detect ProcSignalBarrierPending flag set by the signal handler 
        and calls ProcessProcSignalBarrier to send ack back)
                        ↓
        process sends an "Acknowledgment" back 
        to the Postmaster via shared memory
                        ↓
            The SQL command can safely proceed
    
    *** without CHECK_FOR_INTERRUPTS: the Postmaster will wait forever for this worker (deadlock)
        */
        WaitLatch(MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            20000L,   // check for every 20 sec.
            PG_WAIT_EXTENSION);
        ResetLatch(MyLatch); // for wait again

        // for bgw acknowledge the ProcSignalBarrier from postmaster
        CHECK_FOR_INTERRUPTS();

        if(got_sigterm)
            break;

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        PG_TRY();
        {
            launcher_cleanup();
            launcher_spawn_all_workers();
        }
        PG_CATCH();
        {
            EmitErrorReport();
            FlushErrorState();
        }
        PG_END_TRY();
        
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    elog(LOG, "auto_job launcher shutting down");    
}