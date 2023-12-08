1- git clone https://gitlab.com/heritasgenomica/llm_sql.gi

2- cd llm_sql

3- Por unica vez: En el directorio /scripts, descompactar jira_final.zip

4- Por unica vez: cd ..

5- Por unica vez: docker-compose build

6- docker-compose up

7- en otra terminal de linux:
	  docker exec -it llm_sql_app_1  /bin/bash
	  
	  python3 sql_assistant.py

9-Al terminar:  docker-compose down
