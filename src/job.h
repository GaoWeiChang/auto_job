#pragma once

#include <postgres.h>

#define JOB_STATUS_SCHEDULED "scheduled"
#define JOB_STATUS_RUNNING "running"
#define JOB_STATUS_SUCCESS "success"
#define JOB_STATUS_FAILED "failed"


// register job return job id
extern int job_add(const char *proc_name, int64 schedule_interval);

// run job manually
extern void job_run(int job_id);

// delete job by job id
extern void job_delete(int job_id);

// run the jobs that meet time schedule time
extern int  job_execute_due(void);

// background worker for job
extern void job_worker_main(Datum main_arg);