import argparse
import logging
import time

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


class DataFrameToPostgresql:

    def __init__(self, input, host, port, user, password, database, table, sep, row_sleep_time,
                 source_file_extension, row_size, batch_size, repeat, shuffle, primary_key):
        self.input = input
        print("input: {}".format(self.input))
        self.host = host
        print("host: {}".format(self.host))
        self.port = port
        print("port: {}".format(self.port))
        self.user = user
        print("user: {}".format(self.user))
        self.password = password
        print("password: {}".format(self.password))
        self.database = database
        print("database: {}".format(self.database))
        self.table = table
        print("table: {}".format(self.table))
        self.sep = sep
        print("sep: {}".format(self.sep))
        self.row_sleep_time = row_sleep_time
        print("row_sleep_time: {}".format(self.row_sleep_time))
        self.repeat = repeat
        print("repeat: {}".format(self.repeat))
        self.shuffle = shuffle
        print("shuffle: {}".format(self.shuffle))
        self.df = self.read_source_file(source_file_extension)
        self.row_size = row_size
        print("row_size {}".format(self.row_size))
        self.batch_size = batch_size
        print("batch_size {}".format(self.batch_size))
        self.primary_key = primary_key
        print("primary_key {}".format(self.primary_key))
        self.conn = None
        try:
            logging.info('Connecting to the PostgreSQL')
            conn_str = '{engine}://{user}:{password}@{host}:{port}/{database}'
            self.conn = conn_str.format(**{"host": self.host, "port": self.port,
                                           "database": self.database,
                                           "user": self.user,
                                           "password": self.password,
                                           "engine": "postgresql+psycopg2"})

            print(self.conn)
            logging.info("Connection successfully")
        except Exception as error:
            logging.error("Connection not successfully")
            logging.error(error)
            self.conn = None

    def read_source_file(self, extension='csv'):
        if extension == 'csv':
            if self.shuffle is True:
                df = pd.read_csv(self.input, sep=self.sep).sample(frac=1)
            else:
                df = pd.read_csv(self.input, sep=self.sep)
            df = df.dropna()
            # put all cols into value column
            return df
        # if not csv, parquet
        else:
            if self.shuffle is True:
                df = pd.read_parquet(self.input, 'auto').sample(frac=1)
            else:
                df = pd.read_parquet(self.input, 'auto')
            df = df.dropna()
            # put all cols into value column
            return df

    # Produce a pandas dataframe to postgresql
    def df_to_postgresql(self):
        counter = 0
        df_size = len(self.df) * self.repeat
        total_time = self.row_sleep_time * df_size
        for i in range(0, self.repeat):
            for r in range(0, len(self.df), self.batch_size):
                print(str(r) + "-" + str(r + self.batch_size))
                self.df.loc[r:r + self.batch_size - 1].to_sql(name=self.table, con=self.conn, if_exists='append',
                                                              index=False)
                time.sleep(self.row_sleep_time)
                counter = counter + 1
                remaining_per = 100 - (100 * (counter / df_size))
                remaining_time_secs = (total_time - (self.row_sleep_time * counter))
                remaining_time_mins = remaining_time_secs / 60
                print("%d/%d processed, %s %.2f will be completed in %.2f mins." % (
                    counter, df_size, "%", remaining_per, remaining_time_mins))
        if self.primary_key:
            engine = create_engine(self.conn)
            engine.execute('ALTER TABLE {} ADD COLUMN id SERIAL PRIMARY KEY;'.format(self.table))


if __name__ == "__main__":
    # Boolean options parser
    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')


    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=False, type=str, default="input/iris.csv",
                    help="Source data path. Default: ./input/iris.csv")
    ap.add_argument("-hst", "--host", required=False, type=str, default="localhost",
                    help="Default: localhost")
    ap.add_argument("-p", "--port", required=False, type=str, default="5432",
                    help="Default: 5432")
    ap.add_argument("-u", "--user", required=False, type=str, default="train",
                    help="Default: train")
    ap.add_argument("-psw", "--password", required=False, type=str, default="Ankara06",
                    help="Default: Ankara06")
    ap.add_argument("-db", "--database", required=False, type=str, default="traindb",
                    help="Default: traindb")
    ap.add_argument("-t", "--table", required=False, type=str, default="iris",
                    help="Default: iris")
    ap.add_argument("-s", "--sep", required=False, type=str, default=",",
                    help="Source data file delimiter. Default: ,")
    ap.add_argument("-e", "--source_file_extension", required=False, type=str, default="csv",
                    help="Extension of data file. Default: csv")
    ap.add_argument("-es", "--schema_file_extension", required=False, type=str, default="txt",
                    help="Extension of schema file. Default: txt")
    ap.add_argument("-rst", "--row_sleep_time", required=False, type=float, default=0.5,
                    help="Sleep time in seconds per row. Default: 0.5")
    ap.add_argument("-r", "--repeat", required=False, type=int, default=1,
                    help="How many times to repeat dataset. Default: 1")
    ap.add_argument("-shf", "--shuffle", required=False, type=str2bool, default=False,
                    help="Shuffle the rows?. Default: False")
    ap.add_argument("-rs", "--row_size", required=False, type=int, default=0,
                    help="Default: all_table")
    ap.add_argument("-b", "--batch_size", required=False, type=int, default=10,
                    help="Default: all_table")
    ap.add_argument("-pk", "--primary_key", required=False, type=str2bool, default="no",
                    help="Default: no")
    args = vars(ap.parse_args())

    df_to_postgresql = DataFrameToPostgresql(
        input=args['input'],
        sep=args['sep'],
        row_sleep_time=args['row_sleep_time'],
        source_file_extension=args['source_file_extension'],
        repeat=args['repeat'],
        shuffle=args['shuffle'],
        host=args['host'],
        port=args['port'],
        user=args['user'],
        database=args['database'],
        password=args['password'],
        table=args['table'],
        row_size=args['row_size'],
        batch_size=args['batch_size'],
        primary_key=args['primary_key']
    )
    df_to_postgresql.df_to_postgresql()
