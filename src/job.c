#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <tcop/utility.h>

#include "job.h"



/*
    Private function
*/

// bgw signal handler
static volatile sig_atomic_t got_sigterm = false;

static void
job_scheduler_sigterm_handler(SIGNAL_ARGS)
{
    got_sigterm = true; // received sigterm
    SetLatch(MyLatch); // wake worker up
}

// run exact job
static void
job_run_internal(int job_id, const char *proc_name, int64 schedule_interval)
{
    StringInfoData query;
    int ret;
    TimestampTz now = GetCurrentTimestamp();
    TimestampTz next_run = TimestampTzPlusMilliseconds(now, (schedule_interval/1000));

    // mark job as running
    initStringInfo(&query);
    appendStringInfo(&query,
        "UPDATE _auto_job_catalog.jobs "
        "SET status = '%s', last_run = '%s'::timestamptz "
        "WHERE id = %d",
        JOB_STATUS_RUNNING, timestamptz_to_str(now),
        job_id);
    SPI_execute(query.data, false, 0);

    // execute user procedure
    PG_TRY();
    {
        // procedure
        resetStringInfo(&query);
        appendStringInfo(&query, "CALL %s()", proc_name);
        ret = SPI_execute(query.data, false, 0);
        if(ret < 0)
            ereport(ERROR, (errmsg("job_scheduler: procedure '%s' returned error", proc_name)));

        // mark success and update next run
        resetStringInfo(&query);
        appendStringInfo(&query,
            "UPDATE _auto_job_catalog.jobs "
            "SET status = '%s', next_run = '%s'::timestamptz "
            "WHERE id = %d",
            JOB_STATUS_SUCCESS, timestamptz_to_str(next_run),
            job_id); 
        SPI_execute(query.data, false, 0);
        
        elog(LOG, "job_scheduler: job id=%d ('%s') completed successfully", job_id, proc_name);
    }
    PG_CATCH();
    {
        ErrorData *edata = CopyErrorData();
        
        // mark failed and store error message
        resetStringInfo(&query);
        appendStringInfo(&query,
            "UPDATE _auto_job_catalog.jobs "
            "SET status = '%s', last_error = '%s', next_run = '%s'::timestamptz "
            "WHERE id = %d",
            JOB_STATUS_FAILED, edata->message, timestamptz_to_str(next_run),
            job_id);
        SPI_execute(query.data, false, 0);

        elog(WARNING, "job_scheduler: job id=%d ('%s') failed: %s", job_id, proc_name, edata->message);

        FreeErrorData(edata);
        FlushErrorState();
    }
    PG_END_TRY();
}


/*
    Public function
*/

// register job
int
job_add(const char *proc_name, int64 schedule_interval)
{
    StringInfoData query;
    int ret;
    int job_id = -1;
    TimestampTz now = GetCurrentTimestamp();
    TimestampTz next_run;
    bool isnull;

    next_run = TimestampTzPlusMilliseconds(now, (schedule_interval/1000));
    
    SPI_connect();

    // insert job in catalog
    initStringInfo(&query);
    appendStringInfo(&query,
        "INSERT INTO _auto_job_catalog.jobs "
        "   (proc_name, schedule_interval, next_run, status) "
        "VALUES ('%s', " INT64_FORMAT ", '%s'::timestamptz, '%s') "
        "RETURNING id",
        proc_name, schedule_interval, timestamptz_to_str(next_run), JOB_STATUS_SCHEDULED);

    ret = SPI_execute(query.data, false, 1);
    if(ret != SPI_OK_INSERT_RETURNING || SPI_processed == 0)
        ereport(ERROR, (errmsg("job: failed to insert job '%s'", proc_name)));
    
    job_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    
    SPI_finish();
    
    elog(LOG, "job_scheduler: registered job id=%d proc='%s' interval=" INT64_FORMAT "microseconds", 
        job_id, proc_name, schedule_interval);

    return job_id;
}


// run job manually
void 
job_run(int job_id)
{
    StringInfoData query;
    int ret;
    char *proc_name;
    int64 schedule_interval;
    bool isnull;

    SPI_connect();

    // get job from catalog
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT proc_name, schedule_interval "
        "FROM _auto_job_catalog.jobs "
        "WHERE id = %d", job_id);
    
    ret = SPI_execute(query.data, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
        ereport(ERROR, (errmsg("job_scheduler: job id=%d not found", job_id)));


    proc_name = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    schedule_interval = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
    
    elog(LOG, "Manually triggering job id=%d ('%s')", job_id, proc_name);

    job_run_internal(job_id, proc_name, schedule_interval);

    SPI_finish();
}

// delete job by id
void
job_delete(int job_id)
{
    StringInfoData query;
    int ret;

    SPI_connect();

    initStringInfo(&query);
    appendStringInfo(&query,
        "DELETE FROM _auto_job_catalog.jobs WHERE id = %d", job_id);

    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_DELETE)
        ereport(ERROR, (errmsg("job_scheduler: failed to delete job id=%d", job_id)));

    if (SPI_processed == 0)
        ereport(ERROR, (errmsg("job_scheduler: job id=%d not found", job_id)));
    
    SPI_finish();

    elog(LOG, "job_scheduler: deleted job id=%d", job_id);
}


// run job run when meet the time
int 
job_execute_due(void)
{
    StringInfoData query;
    int ret;
    uint64 n;
    int executed = 0;
    
    // job entry
    typedef struct { 
        int id; 
        char proc[256]; // procedure
        int64 interval; 
    } JobEntry;
    int n_jobs = 0;
    
    // get the non running status jobs
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id, proc_name, schedule_interval "
        "FROM _auto_job_catalog.jobs "
        "WHERE next_run <= NOW() "
        "  AND status <> '%s' "
        "ORDER BY next_run "
        "LIMIT 64", JOB_STATUS_RUNNING);
    
    ret = SPI_execute(query.data, true, 64);
    if (ret != SPI_OK_SELECT){
        SPI_finish();
        return 0;
    }

    // copy result
    n = SPI_processed;
    JobEntry *jobs = (JobEntry *) palloc(n * sizeof(JobEntry));

    for (uint64 i=0; i<n && n_jobs<64; i++){
        bool isnull;

        jobs[i].id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));

        char *proc_val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
        strlcpy(jobs[i].proc, proc_val, 256);

        jobs[i].interval = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull));
        n_jobs++;
    }

    // run each job
    for(int i=0; i<n_jobs; i++){
        job_run_internal(jobs[i].id, jobs[i].proc, jobs[i].interval);
        executed++;
    }

    return executed;
}


// job background worker
void
job_worker_main(Datum main_arg)
{
    Oid db_oid = DatumGetObjectId(main_arg);

    // register signal handler
    pqsignal(SIGTERM, job_scheduler_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnectionByOid(db_oid, InvalidOid, 0);
    pgstat_report_appname("job worker");
    
    while(!got_sigterm){
        // wait 10 sec for receive signal
        int ret = WaitLatch(MyLatch,
                        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                        10000L,  // 10 seconds
                        PG_WAIT_EXTENSION);
        
        ResetLatch(MyLatch);

        CHECK_FOR_INTERRUPTS();

        if(got_sigterm)
            break;

        if(ret & WL_TIMEOUT){
            // apply policy
            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());


            PG_TRY();
            {
                job_execute_due();
            }
            PG_CATCH(); // prevent error from procedure crash
            {
                EmitErrorReport();
                FlushErrorState();
            }
            PG_END_TRY();


            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
        }
    }

    elog(LOG, "job worker shutting down");
}


/*
    Top-level function
*/

PG_FUNCTION_INFO_V1(add_job);
Datum
add_job(PG_FUNCTION_ARGS)
{
    text *proc_name_t = PG_GETARG_TEXT_PP(0); // procedure name
    Interval *interval = PG_GETARG_INTERVAL_P(1); // execution interval
    int64 schedule_interval = interval->time + 
                            ((int64)interval->day * USECS_PER_DAY) + 
                            ((int64)interval->month * (30 * USECS_PER_DAY));

    char *proc_name = text_to_cstring(proc_name_t);
    int job_id;

    job_id = job_add(proc_name, schedule_interval);
    PG_RETURN_INT32(job_id);
}


PG_FUNCTION_INFO_V1(run_job);
Datum
run_job(PG_FUNCTION_ARGS)
{
    int job_id = PG_GETARG_INT32(0);
    job_run(job_id);

    PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(delete_job);
Datum
delete_job(PG_FUNCTION_ARGS)
{
    int job_id = PG_GETARG_INT32(0);
    job_delete(job_id);

    PG_RETURN_VOID();
}

