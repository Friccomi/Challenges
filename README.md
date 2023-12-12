# Data Engineering Coding Challenge


## Problem to resolve:

	  In the context of a DB migration with 3 different tables (departments, jobs, employees) , create
	a local REST API that must:

	1. Receive historical data from CSV files
	2. Upload these files to the new DB
	3. Be able to insert batch transactions (1 up to 1000 rows) with one request

## Architecture:

	Docker-Compose:
	   
	   - Network: 
	   	driver: bridge 
	   	subnet: 177.10.10.0/24
	   	
	   - Two Dockers:
	   	
	   	- PostgreSQL:
	   		IP:177.10.10.10
	   		Port: 5432
	   		Accesible from the local net to be able to inspect tables.
	   		
	   	- Linux Ubuntu, python, FastApi
	   		IP:177.10.10.20
	   		Port: 8000
	   		For accessing APIs: http://177.10.10.20:8000/docs
	   		
## Installation:
	   		
	1- git clone https://github.com/Friccomi/Challenges.git

	2- For only the first time: docker-compose build

	3- docker-compose up

	4- In your browser: http://177.10.10.20:8000/docs/
		
	The Apis runs with any table created in the schema (can be more this three)
	
		a) Delete option, deletes all tables from the schema.
		
		b) Upload: you would be able to upload any table of the schema. Upload at last the ones 
		that have foreign keys, in this example last table to be upload would be hired_employees
	 			
	5- To close all:  docker-compose down

