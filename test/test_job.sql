CREATE TABLE sensor_data (
    ts           TIMESTAMPTZ NOT NULL,
    sensor_name  TEXT        NOT NULL,
    val          FLOAT8
);


-- create procedures
CREATE OR REPLACE PROCEDURE add_sensor_a()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sensor_data (ts, sensor_name, val) VALUES (NOW(), 'sensor_a', random() * 100);
END;
$$;


CREATE OR REPLACE PROCEDURE add_sensor_b()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sensor_data (ts, sensor_name, val) VALUES (NOW(), 'sensor_b', random() * 100);
END;
$$;


-- add jobs
SELECT add_job('add_sensor_a', INTERVAL '30 seconds') AS job_id;
SELECT add_job('add_sensor_b', INTERVAL '20 seconds') AS job_id;


SELECT delete_job(1);  -- remove job_id = 1 
SELECT delete_job(2);  -- remove job_id = 2 


-- check background worker
SELECT pid, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'test_db';

SELECT pid, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'postgres';