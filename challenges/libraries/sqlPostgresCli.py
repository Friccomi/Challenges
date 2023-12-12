from sqlalchemy import create_engine, inspect
from decouple import config

import sqlalchemy

import libraries.sqlConnexion as con
from sqlalchemy.ext.declarative import declarative_base


class SqlPostgresClient(con.SqlConnexionClient):
    """SqlPostgresClient is a class to connect to a postgresql database

    parameter are taken from the .env file

    Functions:
    ---------
     _get_engine,

     _connect,

     _cursor_columns,

     execute,

     insert_from_frame,

     to_frame


    """

    def __init__(self):
        super().__init__()

        self.user = config("DB_USER")
        self.password = config("DB_PASSWORD")
        self.host = config("DB_HOST")
        self.port = config("DB_PORT")

        self.schema = config("DB_SCHEMA")
        print(self.host)
        self.autocommit = config("DB_AUTOCOMMIT", cast=bool)
        self.db = f"{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    #   print("en postgresCli")
    #    print(f"{self.dialect}://{self.db}")

    def _get_engine(self):
        db_uri = f"{self.dialect}://{self.db}"

        if not self.engine:
            self.engine = create_engine(
                db_uri,
                connect_args={"options": "-csearch_path={}".format(self.schema)},
            )

        # self.engine.execution_options(autocommit=True)
        return self.engine

    def _connect(self):
        return self._get_engine().connect()

    def table_exist(self, tableName, connection=None, **kwargs):
        if connection is None:
            connection = self._connect()
        engine = self._get_engine()
        insp = inspect(engine)
        if insp.has_table(tableName, schema=self.schema):
            return True
        else:
            return False

    def return_table_columns(self, tableName, **kwargs):
        engine = self._get_engine()
        insp = inspect(engine)
        columns_table = insp.get_columns(tableName, self.schema)  # schema is optional
        return columns_table

    def return_table_names(self, **kwargs):
        engine = self._get_engine()
        insp = inspect(engine)
        tableNames = insp.get_table_names(schema=self.schema)  # schema is optional
        return tableNames


# if __name__ == "__main__":
#   connect_cli = SqlPostgresClient()
#  print(connect_cli.to_frame("select * from avg_delays"))
