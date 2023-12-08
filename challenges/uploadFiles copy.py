from fastapi import FastAPI, File, UploadFile
from typing import List
import uvicorn
import libraries._general as grl
from libraries.sqlPostgresCli import SqlPostgresClient
import libraries.s3_conn as conn
from decouple import config
import os
import shutil
import csv
from fastapi import HTTPException
import codecs


s3, bucket_name_s3, region, destiny, origin = grl.obtain_buckets_data()

sql_cli = SqlPostgresClient()

path = config("PATH_APP")
prefix = config("PATH_FILES")


app = FastAPI()


@app.post("/upload")
def upload(files: List[UploadFile] = File(...)):
    for file in files:
        try:
            # csvReader = csv.DictReader(codecs.iterdecode(file.file, 'utf-8'))

            name = file.filename
            print(type(name))
            with open(file.filename, "w") as f:
                while contents := file.file.read(1024 * 1024):
                    f.write(contents)
        except Exception:
            return {"message": "There was an error uploading the file(s)"}
        finally:
            file.file.close()
            filename = f'{os.getenv("HOME")}/GO/filesGo/{name}'
            shutil.copy2(
                file.filename,
                filename,
            )
            if is_csv(filename) == False:
                raise HTTPException(400, f"{file.filename} is not a csv file")

            os.unlink(file.filename)

    return {"message": f"Successfuly uploaded {[file.filename for file in files]}"}


def is_csv(infile):
    try:
        if infile.split(".")[1] != "csv":
            return False
        with open(infile, newline="") as csvfile:
            print(csvfile)
            start = csvfile.read(4096)

            # isprintable does not allow newlines, printable does not allow umlauts...
            if not all([c in string.printable or c.isprintable() for c in start]):
                return False
            dialect = csv.Sniffer().sniff(start)
            return True
    except csv.Error:
        # Could not get a csv dialect -> probably not a csv.
        return False
    except Exception as e:
        print(e)
        return False


def is_csv_N(infile):
    try:
        if infile.split(".")[1] != "csv":
            return False
        csv_fileh = open(infile, "rb")
        csv_fileh.seek(0)
        dialect = csv.Sniffer().sniff(csv_fileh.read(1024))
        # Perform various checks on the dialect (e.g., lineseparator,
        # delimiter) to make sure it's sane

        # Don't forget to reset the read position back to the start of
        # the file before reading any entries.
        csv_fileh.seek(0)
        return True
    except csv.Error:
        # Could not get a csv dialect -> probably not a csv.
        return False
    except Exception as e:
        print(e)
        return False


if __name__ == "__main__":
    uvicorn.run("uploadFiles:app", port=5000, log_level="info")
    # in the browser type  -->  http://127.0.0.1:5000/docs
