## Overview

Auto job extension is the postgreSQL extension that schedules and runs stored procedures automatically using background workers.
- This extension lets you register PostgreSQL procedures to run on a recurring interval. Once registered, jobs run automatically in the background — no external cron daemon or application-side scheduling needed.
- Each registered job is tracked in a catalog table with its status, last run time, next run time, and any error messages.

## The Launcher Pattern

PostgreSQL background workers (BGWs) must be registered at load time (in `_PG_init`) or dynamically at runtime. The problem is that `_PG_init` runs once when the library loads, **it cannot know which databases have the extension installed, and a single static BGW can only connect to one database**.

### How the launcher solves this

<p align="center">
  <img src="https://github.com/user-attachments/assets/9707e4a0-b70a-4fa8-a362-64c4867fc4fb" width="757">
</p>

1. One launcher BGW is registered statically in `_PG_init`. It connects to the always-available `postgres` database.
2. When extension is run in any user database, the SQL script registers that database in a shared `public.auto_job_registry` table (via `dblink`).
3. On each poll cycle, the launcher spawn exactly one job worker per registered database.
4. Each job worker connects to its assigned database.

### Why the launcher pattern matters for BGW design

| Concern | How the launcher handles it |
|---|---|
| Multi-database support | One launcher manages workers across all databases from a single stable connection |
| Crash recovery | Job workers use `BGW_NEVER_RESTART`. If a database crashes and kills its worker, the launcher detects the missing `pg_stat_activity` entry and re-spawns it after recovery |
| Extension uninstall | Every cycle the launcher checks for databases where `auto_job` was dropped and terminates the orphaned worker + removes the registry entry |
| Launcher self-recovery | The postmaster restarts the launcher on crash (eg. `bgw_restart_time = 5` seconds) |


## Getting Started
### System Requirements
- **PostgreSQL 17**: Check with `psql --version` 
- **CMake**: 3.10 or higher. Check with `cmake --version`

### Installation
Add to `postgresql.conf`
```
shared_preload_libraries = 'auto_job'
```

Compile and install
```
sudo ./build.sh
```

Connect to `postgres` database and attach `dblink` extension
```
psql -U postgres

CREATE EXTENSION dblink;
```

Check `launcher` background worker in `postgres` database
```
SELECT pid, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'postgres';
```
<p align="center">
  <img width="822" height="159" alt="image" src="https://github.com/user-attachments/assets/ba28fd5e-c140-4d2d-a62e-8bb5c95c0c02" />
</p>

## Usage

### Create extension
```
-- connect to another database
\c test_db

-- create auto_job extension
CREATE EXTENSION auto_job CASCADE;
```

### Check `auto_job` background worker
```
SELECT pid, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'test_db';
```
<p align="center">
  <img width="822" height="158" alt="image" src="https://github.com/user-attachments/assets/c8f44512-0faa-4eef-9eec-60b531a258ca" />
</p>

### Register a job

```
-- create sensor data table
CREATE TABLE sensor_data (
    ts           TIMESTAMPTZ NOT NULL,
    sensor_name  TEXT        NOT NULL,
    val          FLOAT8
);

-- create procedure
CREATE OR REPLACE PROCEDURE add_sensor_a()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sensor_data (ts, sensor_name, val) VALUES (NOW(), 'sensor_a', random() * 100);
END;
$$;


-- schedule it to run every hour
SELECT add_job('sensor_data', INTERVAL '1 hour');
```

### Inspect jobs
```
SELECT * FROM _auto_job_catalog.job_info;
```
<p align="center">
  <img width="1430" height="120" alt="image" src="https://github.com/user-attachments/assets/9d68008f-360d-4330-b502-533091d7e5b3" />
</p>

- Job status

| Status | Meaning |
|---|---|
| `scheduled` | Waiting for next run time |
| `running` | Currently executing |
| `success` | Last run completed without error |
| `failed` | Last run raised an error (see `last_error` column) |


### Run a job manually
```
-- Run job id=1 immediately, regardless of its schedule
SELECT run_job(1);
```

### Delete a job
```
SELECT delete_job(1);
```

### Check all database that attached `auto_job` extension
```
-- connect to "postgres" database
\c postgres

select * from auto_job_registry;
```
<img width="510" height="94" alt="image" src="https://github.com/user-attachments/assets/d05dcb10-d535-4e18-a4eb-7fe94d0d4903" />


## Crash & Restart Demonstration
Simulate worker revive after database crash
- check worker is running
<img width="810" height="159" alt="image" src="https://github.com/user-attachments/assets/f19fccec-fe6a-439f-bd3b-043bc4574722" />

- change to root user
  - crash database: `systemctl kill -s SIGKILL postgresql`
  - restart database: `systemctl restart postgresql`

- the worker get respawn after database crash
```
SELECT pid, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'test_db';
```
<img width="813" height="159" alt="image" src="https://github.com/user-attachments/assets/ad27deb0-a2de-4cb7-9cec-e10ba6086487" />

