from locale import D_FMT
import pandas as pd
import libraries.s3_conn as conn
from io import StringIO
import os
import io
import json
from datetime import datetime as dt
from decouple import config
import sqlalchemy
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    exc,
)
from libraries.sqlPostgresCli import SqlPostgresClient

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook


SCHEMA = config("DB_SCHEMA")
# sql_cli = SqlPostgresClient()
path = config("PATH_APP")
prefix = config("PATH_FILES")


def _save_in_DB(df1, SQL_TABLE):
    print("Antes de Grabar")
    try:
        sql_cli.insert_from_frame(df1, SQL_TABLE)
        print(f"Inserted {len(df1)} records")
    except sqlalchemy.exc.IntegrityError as ee:
        # You can avoid doing this by setting a trigger rule in the reports operator
        print(f"Inserting Data on {SQL_TABLE}: {ee}")
        raise
        with open("readme.txt", "w") as f:
            f.write(str(ee))


def limpiar_tablas(tablas):  # porceso 1.2
    try:  # if table pre-exist, delete all data
        sql_cli.execute(f"truncate table {tablas}")

    except sqlalchemy.exc.IntegrityError as error:
        print(str(error.orig) + " for parameters" + str(error.params))


def save_df(origin, bucket, prefix, filename, df_final, type):
    print("save_df")
    if origin == "s3":
        print(filename)
        filename0 = f"{prefix}{filename}"
        extension = filename.split(".")[1]
        csv_buffer = StringIO()
        print(extension)
        if extension == "json":
            df_final.to_json(csv_buffer)
        else:
            df_final.to_csv(csv_buffer, index=False)

        # s3 = S3Hook(aws_conn_id=config("CONN_ID_S3")).get_session().resource("s3")
        #  s3.Object(bucket, filename0).put(Body=csv_buffer.getvalue())
        print("paso")
    else:
        filename = os.path.join("", prefix, filename)
        print(filename)
        extension = filename.split(".")[1]
        print(extension)
        if extension == "json":
            df_final.to_json(f"{filename}")
        else:
            df_final.to_csv(f"{filename}", index=False)


def obtain_file(origin, prefix, filename, bucket, type):
    print("Obtain_file")
    if origin == "s3":
        print("Antes de Leer")
        filename0 = f"{prefix}{filename}"
        extension = filename.split(".")[1]
        if extension == "json":
            pass
        #    df = pd.read_json(
        #       io.StringIO(
        #            S3Hook(aws_conn_id=config("CONN_ID_S3")).read_key(filename0, bucket)
        #        )
        #    )
        else:
            pass
        #   df = pd.read_csv(
        #       io.StringIO(
        #           S3Hook(aws_conn_id=config("CONN_ID_S3")).read_key(filename0, bucket)
        #       )
        #   )
        print("leido")
        df = df.reindex()
    else:
        print(prefix)
        print(filename)
        filename = os.path.join("", prefix, filename)
        extension = filename.split(".")[1]
        print(extension)
        if extension == "json":
            df = pd.read_json(filename)
        else:
            df = pd.read_csv(filename)
    return df


def obtain_json_file(origin, prefix, filename, bucket, type):
    print("Obtain_file")
    if origin == "s3":
        print("Antes de Leer")
        filename0 = f"{prefix}{filename}"
        extension = filename.split(".")[1]
        if extension == "json":
            pass
        #    json_file = json.load(
        #        io.StringIO(
        #            S3Hook(aws_conn_id=config("CONN_ID_S3")).read_key(filename0, bucket)
        #        )
        #    )
    else:
        print(prefix)
        print(filename)
        filename = os.path.join("", prefix, filename)
        extension = filename.split(".")[1]
        print(extension)
        if extension == "json":
            with open(filename, "r") as f:
                json_file = json.load(f)
    return json_file


# def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
#    hook = S3Hook(aws_conn_id=config("CONN_ID_S3"))
#    try:
#        hook.load_file(
#            filename=filename, key=key, bucket_name=bucket_name, replace=True
#        )
#    except Exception as X:
#        print(X)
#        raise


def obtain_buckets_data():
    destiny = conn.destiny
    origin = conn.origin
    bucket_name = ""
    s3 = ""
    region = ""
    if origin == "s3":
        region = conn.AWS_REGION
        bucket_name = conn.AWS_BUCKET_NAME
    #    s3_hook = S3Hook(aws_conn_id=config("CONN_ID_S3"))
    #    try:
    #       response_file_1 = s3_hook.check_for_bucket(
    #            conn.AWS_BUCKET_NAME
    #        )  # chequea que se pueda conectar
    #       s3 = S3Hook(config("CONN_ID_S3")).get_conn()
    #   except:
    #       print(f"Imposible to connect bucket {config('CONN_ID_S3')}")

    return s3, bucket_name, region, destiny, origin


# from airflow import DAG
# from airflow.models import DagRun
# from airflow.utils.email import send_email
# from airflow.operators.python import PythonOperator


# def check_task_failure_count(dag_id, task_id):
##
#    Args:
#        dag_id (str): The dag_id to check
#        task_id (str): The task_id to check
#    Returns:
#        List - The number of times a task has failed in the last dag run
#    """
#    last_dag_run = DagRun.find(dag_id=dag_id)
#    last_dag_run.sort(key=lambda x: x.execution_date, reverse=True)
#    return last_dag_run[0].get_task_instance(task_id).try_number


# def failture_email(dag_id, task_id, **context):
#    """Emails when a task id fails 3 times

#   Args:
#       dag_id (str): The dag_id to check
#        task_id (str): The task_id to check
#        context (dict): The dag_run context
#   """
# retained this for posterity, however it is not used in this example
# instead we use the retries value from the DAG context via 'retries' : 3
#    last_dag_run = DagRun.find(dag_id=dag_id)
#   last_dag_run.sort(key=lambda x: x.execution_date, reverse=True)
#    retries = last_dag_run[0].get_task_instance(task_id).try_number

#    msg = "DAG/TASK Failed!"
#    subject = f"DAG {dag_run} has failed"
#    send_email(to="2222@yahoo.com.ar", subject=subject, html_content=msg)
