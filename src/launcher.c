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



/*
    Overview
    --------
    The launcher runs as a single background worker in the "postgres" database.
    Every 20 seconds it:
      1. Calls launcher_cleanup()        — removes registry entries for databases
                                           that were dropped or had the extension
                                           uninstalled.
      2. Calls launcher_spawn_all_workers() — iterates auto_job_registry and
                                           ensures exactly one "job worker" BGW
                                           is running per registered database.

    Surviving a database crash / restart
    -------------------------------------
    Job workers are registered with bgw_restart_time = BGW_NEVER_RESTART, so
    the postmaster will NOT automatically restart them after a crash.  Instead,
    the launcher acts as the watchdog:

      - spawn_worker() calls is_worker_running(), which queries pg_stat_activity
        to check whether a "job worker" with the target datid is already alive.
      - If the database crashed (killing its job worker), the next poll cycle
        finds no matching entry in pg_stat_activity and calls
        RegisterDynamicBackgroundWorker() to spawn a fresh one.
      - Because the launcher itself connects to the always-available "postgres"
        database (not the user database), it keeps running even while a user
        database is down, so it can detect recovery and re-spawn the worker
        as soon as the database comes back up.

    The postmaster is responsible for restarting the launcher itself if it ever
    crashes (configured at registration time in _PG_init).
*/



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
    worker.bgw_restart_time = BGW_NEVER_RESTART; // launcher will spawn this worker instead
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
        elog(LOG, "auto_job launcher: dropped job worker for db oid=%u", db_oid);

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
    uint64 n;
    Oid *db_oids;

    ret = SPI_execute(
        "SELECT db_oid FROM public.auto_job_registry",
        true, 0);
    
    if(ret != SPI_OK_SELECT){
        elog(WARNING, "auto_job launcher: fail to query databases");
        return;
    }

    n = SPI_processed;
    if(n == 0)
        return;
    
    db_oids = palloc(n * sizeof(Oid));
    for(uint64 i=0; i < n; i++){
        bool isnull;
        db_oids[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
    }

    for(uint64 i=0; i < n; i++){
        spawn_worker(db_oids[i]);
    }
}


/*
    Public function
*/

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

        /*  Interrupt handling logic

            When executing certain SQL commands, a backend process calls 
            SendProcSignalBarrier() to request a global barrier.

            The postmaster ensures that all background workers receive
            the signal (SIGUSR1). Each worker processes it via CHECK_FOR_INTERRUPTS() 
            and then sends an acknowledgment.

            The backend waits until all workers acknowledge before proceeding.

            *** WARNING: without CHECK_FOR_INTERRUPTS() here will cause the
                backend to wait forever (deadlock)
        */

        WaitLatch(MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            20000L,   // check for every 20 sec.
            PG_WAIT_EXTENSION);
        ResetLatch(MyLatch); // for wait again

        // bgw acknowledge
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