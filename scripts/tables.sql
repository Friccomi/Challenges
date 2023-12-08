CREATE SCHEMA challenge AUTHORIZATION postgres;
-- Drop table

-- DROP TABLE exercise.departments;

CREATE TABLE challenge.departments (
	id int4 NOT NULL,
	department varchar NULL,
	CONSTRAINT departments_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE challenge.departments OWNER TO postgres;
GRANT ALL ON TABLE challenge.departments TO postgres;


-- exercise.jobs definition

-- Drop table

-- DROP TABLE exercise.jobs;

CREATE TABLE challenge.jobs (
	id int4 NOT NULL,
	job varchar NULL,
	CONSTRAINT jobs_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE challenge.jobs OWNER TO postgres;
GRANT ALL ON TABLE challenge.jobs TO postgres;


-- exercise.hired_employees definition

-- Drop table

-- DROP TABLE exercise.hired_employees;

CREATE TABLE challenge.hired_employees (
	id int4 NOT NULL,
	"name" varchar NULL,
	datetime timestamp NULL,
	department_id int4 NULL,
	job_id int4 NULL,
	CONSTRAINT hired_employees_pk PRIMARY KEY (id),
	CONSTRAINT hired_employees2_fk FOREIGN KEY (job_id) REFERENCES challenge.jobs(id),
	CONSTRAINT hired_employees_fk FOREIGN KEY (department_id) REFERENCES challenge.departments(id)
);

-- Permissions

ALTER TABLE challenge.hired_employees OWNER TO postgres;
GRANT ALL ON TABLE challenge.hired_employees TO postgres;




-- Permissions

GRANT ALL ON SCHEMA challenge TO postgres;
   
   
