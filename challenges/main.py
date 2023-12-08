import codecs
from fastapi import FastAPI, File, UploadFile, HTTPException
from typing import List
import uvicorn
import pandas as pd
from decouple import config
import os
import csv
import libraries._general as grl
from libraries.sqlPostgresCli import SqlPostgresClient

s3, bucket_name_s3, region, destiny, origin = grl.obtain_buckets_data()

sql_cli = SqlPostgresClient()

path = config("PATH_APP")
prefix = config("PATH_FILES")
sql_cli = SqlPostgresClient()

app = FastAPI()


@app.post("/upload")
def upload(files: List[UploadFile] = File(...)):
    for file in files:
        try:
            try:
                name = file.filename
                data = file.file
                data = csv.reader(codecs.iterdecode(data, "utf-8"), delimiter=",")
                df = pd.DataFrame(data)
                prefix1 = f'{os.getenv("HOME")}{prefix}'
                grl.save_df(destiny, bucket_name_s3, prefix1, name, df, "csv")
            except Exception:
                raise ValueError(f"{file.filename} is not a csv file")
            try:
                uploadToDB(prefix1)
            except Exception:
                raise HTTPException(422, f"{file.filename} is not a valida table")
        except ValueError as exp:
            print(exp)
            raise HTTPException(422, exp)

    return {"message": f"Successfuly uploaded {[file.filename for file in files]}"}


def uploadToDB(prefix_v):
    print(f"En el else: {prefix_v}")
    filelist = [f for f in os.listdir(prefix_v)]
    for f in filelist:
        fileName = os.path.join(prefix_v, f)
        tableName = f.split(".csv")[0]
        print(tableName)
        if not sql_cli.table_exist():
            raise


if __name__ == "__main__":
    uvicorn.run("main:app", port=5000, log_level="info")
    # in the browser type  -->  http://127.0.0.1:5000/docs
