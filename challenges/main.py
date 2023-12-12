import codecs
from fastapi import FastAPI, File, UploadFile, HTTPException
from typing import List
import uvicorn
import pandas as pd
from decouple import config
import os
import csv
import datetime
from sqlalchemy import text
import sqlalchemy as sa
import numpy as np
from datetime import datetime
from datetime import timezone
import libraries._general as grl
from libraries.sqlPostgresCli import SqlPostgresClient

s3, bucket_name_s3, region, destiny, origin = grl.obtain_buckets_data()

sql_cli = SqlPostgresClient()

path = config("PATH_APP")
prefix = config("PATH_FILES")
prefix_2 = config("PATH_FILES2")
sql_cli = SqlPostgresClient()

app = FastAPI()


@app.post("/upload")
def upload(files: List[UploadFile] = File(...)):
    sigue = "s"
    prefix1 = f'{os.getenv("HOME")}{prefix}'
    prefix2 = f'{os.getenv("HOME")}{prefix_2}'
    limpiarDirectorio(prefix1)
    limpiarDirectorio(prefix2)
    for file in files:
        try:
            name = file.filename
            data = file.file
            data = csv.reader(codecs.iterdecode(data, "utf-8"), delimiter=",")

            df_table = pd.DataFrame(data)
            grl.save_df(destiny, bucket_name_s3, prefix1, name, df_table, "csv")
        except Exception:
            sigue = "n"
            HTTPException(421, f"{file.filename} is not a csv file")
    if sigue == "s":
        try:
            mensaje = uploadToDB(prefix1, prefix2, df_table)
            if mensaje != "":
                raise
        except Exception:
            raise HTTPException(422, f"{file.filename.split('.csv')[0]} {mensaje}")

    return {"message": f"Successfuly uploaded {[file.filename for file in files]}"}


def limpiarDirectorio(prefix_v):
    filelist = [f for f in os.listdir(prefix_v)]
    for f in filelist:
        os.remove(os.path.join(prefix_v, f))


def uploadToDB(prefix_v, prefix_v2, df_table):
    filelist = [f for f in os.listdir(prefix_v)]
    # disable_constraints()
    for f in filelist:
        fileName = os.path.join(prefix_v, f)
        tableName = f.split(".csv")[0]
        print(tableName)
        try:
            if not sql_cli.table_exist(tableName):
                #  enable_contraints()
                return "is not a valida table"
        except Exception as error:
            # enable_contraints()
            return error
        try:
            columns_table = sql_cli.return_table_columns(tableName)
            df_col_tab = pd.DataFrame(columns_table)
        except Exception as error:
            # enable_contraints()
            return error
        header = []
        print(header)
        print()
        for c in columns_table:
            header.append(c["name"])

        df_table = pd.DataFrame(
            np.row_stack([df_table.columns, df_table.values]), columns=header
        )
        df_table = df_table.drop([0], axis=0)
        print(df_table)
        grl.save_df(
            destiny, bucket_name_s3, prefix_v2, f"{tableName}_aux.csv", df_table, "csv"
        )

        for c in columns_table:
            print(c["name"])
            print(c["type"])
            columName = c["name"]
            if str(c["type"]) == "INTEGER":
                df_table[columName] = pd.to_numeric(df_table[columName])
            if str(c["type"]) == "TIMESTAMP":
                df_table[columName] = pd.to_datetime(df_table[columName])
        try:
            grl.limpiar_tablas(tableName)
            # grl._save_in_DB(df_table, tableName)

            batch_size = 1000
            for batch_number, batch_df in df_table.groupby(
                np.arange(len(df_table)) // batch_size
            ):
                grl._save_in_DB(batch_df, tableName)
        except Exception as error:
            # enable_contraints()
            mensaje = error
            return mensaje

    # enable_contraints()
    return ""


def enable_contraints():  # the correct to do is SET session_replication_role = 'replica'; but is not working
    sql_cli.execute(
        "ALTER TABLE challenge.hired_employees ADD CONSTRAINT hired_employees_fk FOREIGN KEY (department_id) REFERENCES challenge.departments(id); ALTER TABLE challenge.hired_employees ADD CONSTRAINT hired_employees_fk_1 FOREIGN KEY (job_id) REFERENCES challenge.jobs(id); commit;"
    )


def disable_constraints():  # the correct to do is SET session_replication_role = 'replica'; but is not working
    sql_cli.execute(
        "ALTER TABLE challenge.hired_employees DROP CONSTRAINT hired_employees_fk; ALTER TABLE challenge.hired_employees DROP CONSTRAINT hired_employees_fk_1;"
    )


if __name__ == "__main__":
    uvicorn.run("main:app", port=5000, log_level="info")
    # in the browser type  -->  http://127.0.0.1:5000/docs
    # sql_cli.table_exist("jobs")
    # connect_cli = SqlPostgresClient()
    #  print(connect_cli.to_frame(text("select * from challenge.jobs")))
    # df = sql_cli.to_frame(f"select * from challenge.jobs")
    #  print(df)
# prefix1 = f'{os.getenv("HOME")}{prefix}'
# prefix2 = f'{os.getenv("HOME")}{prefix_2}'
# df = grl.obtain_file(
#     origin, prefix2, "hired_employees_aux.csv", bucket_name_s3, "csv"
# )
# grl.limpiar_tablas(""hired_employees_aux")
# grl._save_in_DB(df, "jobs")
#  mensaje = uploadToDB(prefix1, prefix2, df)
#   print(mensaje)
#  columns_table = sql_cli.return_table_columns("jobs")
# grl._save_in_DB(df, "jobs")
