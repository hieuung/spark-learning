CREATE SCHEMA IF NOT EXISTS lake;

CREATE TABLE IF NOT EXISTS lake.job (
  id SERIAL PRIMARY KEY,
  db_name VARCHAR(45),
  table_name VARCHAR(45),
  action VARCHAR(255),
  last_row_number INTEGER,
  status VARCHAR(45),
  details JSON,
  zone VARCHAR(10),
  job_date VARCHAR(10),
  job_start TIMESTAMP,
  job_end TIMESTAMP
);
