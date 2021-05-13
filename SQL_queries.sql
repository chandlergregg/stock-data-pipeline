-- Create schema
CREATE SCHEMA IF NOT EXISTS stock_proj;

-- Create tracker table
CREATE TABLE IF NOT EXISTS stock_proj.pipeline_job_status (
	job_status_id SERIAL PRIMARY KEY
	, job_name VARCHAR(50) NOT NULL
	, job_status_timestamp TIMESTAMP NOT NULL
	, job_status VARCHAR(50) NOT NULL
);

-- Test select
SELECT * FROM stock_proj.pipeline_job_status 
ORDER BY job_status_timestamp DESC
LIMIT 100;

-- Insert statement
INSERT INTO stock_proj.pipeline_job_status
(job_name, job_status_timestamp, job_status)
VALUES
('Ingest data', now(), 'Done');

-- Check to see if inserted
SELECT * FROM stock_proj.pipeline_job_status LIMIT 10;

-- Insert again
INSERT INTO stock_proj.pipeline_job_status
(job_name, job_status_timestamp, job_status)
VALUES
('EOD load', now(), 'In progress');