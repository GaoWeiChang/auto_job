-- ==========================================
-- Job Table/View/Schema
-- ==========================================

CREATE SCHEMA _auto_job_catalog;
REVOKE ALL ON SCHEMA _auto_job_catalog FROM PUBLIC;

-- Job catalog, store job metadata
CREATE TABLE _auto_job_catalog.jobs (
    id                SERIAL          PRIMARY KEY,
    proc_name         TEXT            NOT NULL,          -- procedure name
    schedule_interval BIGINT          NOT NULL,          -- schedule interval (microseconds)
    next_run          TIMESTAMPTZ     NOT NULL,          -- next time run
    last_run          TIMESTAMPTZ,                       -- last time run (NULL = didnt run yet)
    last_error        TEXT,                              -- last error message (NULL = no error)
    status            TEXT            NOT NULL
                                    DEFAULT 'scheduled'
                                    CHECK (status IN ('scheduled', 'running', 'success', 'failed')),
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index for background worker to find job
CREATE INDEX jobs_next_run_idx
    ON _auto_job_catalog.jobs (next_run)
    WHERE status <> 'running';

-- job information view
CREATE VIEW _auto_job_catalog.job_info AS
SELECT
    j.id AS job_id,
    j.proc_name,
    (j.schedule_interval * INTERVAL '1 microsecond') AS schedule_interval,
    j.next_run,
    j.last_run,
    j.status,
    j.last_error,
    j.created_at
FROM _auto_job_catalog.jobs j
ORDER BY j.id;


-- ==========================================
-- Functions
-- ==========================================

-- add new job
CREATE FUNCTION add_job(
    proc_name            TEXT,
    schedule_interval    INTERVAL
) RETURNS INTEGER
AS 'MODULE_PATHNAME', 'add_job'
LANGUAGE C STRICT;
COMMENT ON FUNCTION add_job(TEXT, INTERVAL) IS
    'Register a new scheduled job, return the assigned job_id';


-- run the job manually
CREATE FUNCTION run_job(
    job_id      INTEGER
) RETURNS VOID
AS 'MODULE_PATHNAME', 'run_job'
LANGUAGE C STRICT;
COMMENT ON FUNCTION run_job(INTEGER) IS
    'Immediately execute a job regardless of its next_run schedule';


-- delete the job
CREATE FUNCTION delete_job(
    job_id INTEGER
) RETURNS VOID
AS 'MODULE_PATHNAME', 'delete_job'
LANGUAGE C STRICT;
COMMENT ON FUNCTION delete_job(INTEGER) IS
    'Remove a job from the scheduler by job_id';


-- ==========================================
-- Registry for postgres db
-- ==========================================

-- registry table on postgres database via dblink
SELECT dblink_exec(
    'dbname=postgres',
    $SQL$
        CREATE TABLE IF NOT EXISTS public.auto_job_registry (
            db_oid   OID  PRIMARY KEY,
            db_name  TEXT NOT NULL,
            registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    $SQL$
);

-- register postgres in registry table
SELECT dblink_exec(
    'dbname=postgres',
    format(
        $SQL$
            INSERT INTO public.auto_job_registry (db_oid, db_name)
            VALUES (%s, %L)
            ON CONFLICT (db_oid) DO NOTHING
        $SQL$,
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database()
    )
);

