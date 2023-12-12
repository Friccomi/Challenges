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
prefix1 = f"{path}{prefix}"
prefix2 = f"{path}{prefix_2}"
# prefix1 = f"/home/flor/GO/filesGo/"
# prefix2 = f"/home/flor/GO/filesGoAux/"


@app.delete("/")
def delete_tables():
    tableNames = sql_cli.return_table_names()
    for tableName in tableNames:
        try:
            grl.limpiar_tablas(tableName)
        except Exception as error:
            raise HTTPException(
                status_code=404, detail=f"Unable to delete table {tableName}: {error}"
            )
    return {"ok": True}


@app.post("/upload")
def upload(files: List[UploadFile] = File(...)):
    sigue = "s"
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
            mensaje = uploadToDB(prefix1, prefix2)
            if mensaje != "":
                raise
        except Exception:
            raise HTTPException(422, f"{file.filename.split('.csv')[0]} {mensaje}")

    return {"message": f"Successfuly uploaded {[file.filename for file in files]}"}


def limpiarDirectorio(prefix_v):
    filelist = [f for f in os.listdir(prefix_v)]
    for f in filelist:
        os.remove(os.path.join(prefix_v, f))


def uploadToDB(prefix_v, prefix_v2):
    filelist = [f for f in os.listdir(prefix_v)]
    for f in filelist:
        fileName = os.path.join(prefix_v, f)
        df_table = grl.obtain_file(origin, prefix_v2, fileName, bucket_name_s3, "csv")
        tableName = f.split(".csv")[0]
        try:
            if not sql_cli.table_exist(tableName):
                return "is not a valida table"
        except Exception as error:
            return error
        try:
            columns_table = sql_cli.return_table_columns(tableName)
            df_col_tab = pd.DataFrame(columns_table)
        except Exception as error:
            return error
        header = []
        for c in columns_table:
            header.append(c["name"])

        df_table = pd.DataFrame(
            np.row_stack([df_table.columns, df_table.values]), columns=header
        )
        df_table = df_table.drop([0], axis=0)
        grl.save_df(
            destiny, bucket_name_s3, prefix_v2, f"{tableName}_aux.csv", df_table, "csv"
        )

        for c in columns_table:
            columName = c["name"]
            if str(c["type"]) == "INTEGER":
                df_table[columName] = pd.to_numeric(df_table[columName])
            if str(c["type"]) == "TIMESTAMP":
                df_table[columName] = pd.to_datetime(df_table[columName])
        try:
            grl.limpiar_tablas(tableName)
            batch_size = 1000
            for batch_number, batch_df in df_table.groupby(
                np.arange(len(df_table)) // batch_size
            ):
                grl._save_in_DB(batch_df, tableName)
        except Exception as error:
            mensaje = error
            return mensaje

    return ""


if __name__ == "__main__":
    uvicorn.run("main:app", port=5000, log_level="info")
