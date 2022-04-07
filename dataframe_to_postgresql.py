import argparse
import time
import pandas as pd
from sqlalchemy import create_engine




class DataFrameToPostgreSQL:

    def __init__(self, input, host, user, password, database, table, sep, row_sleep_time,
                 source_file_extension,
                 repeat, shuffle, key_index):
        self.input = input
        print("input: {}".format(self.input))
        self.host = host
        print("host: {}".format(self.host))
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
        self.key_index = key_index
        print("key_index: {}".format(self.key_index))
        self.conn = None
        try:
            print('Connecting to the PostgreSQL...........')
            conn_str = '{engine}://{user}:{password}@{host}:{port}/{database}'
            self.conn = create_engine(conn_str.format(**{"host": self.host, "port": 5432,
                                                         "database": self.database,
                                                         "user": self.user,
                                                         "password": self.password,
                                                         "engine": "postgresql+psycopg2"}))
            print("Connection successfully..................")
        except Exception as error:
            print(error)
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
        sayac = 0
        df_size = len(self.df) * self.repeat
        total_time = self.row_sleep_time * df_size
        for i in range(0, self.repeat):
            for index, row in self.df.iterrows():
                if self.key_index == 1000:
                    self.df.to_sql(name=self.table, con=self.conn, if_exists='append', index=False)
                    # row[-1] corresponds to all columns which already put in one column named value
                    # If  -k or --key_index not used pandas df index will be sent to kafka as key
                else:
                    self.df.to_sql(name=self.table, con=self.conn, if_exists='append', index=False)
                    # if -k or --key_index used the column spesified in this option will be sent to kafka as key
                time.sleep(self.row_sleep_time)
                sayac = sayac + 1
                remaining_per = 100 - (100 * (sayac / df_size))
                remaining_time_secs = (total_time - (self.row_sleep_time * sayac))
                remaining_time_mins = remaining_time_secs / 60
                print(str(index) + " - " + str(row[-1]))
                print("%d/%d processed, %s %.2f will be completed in %.2f mins." % (
                    sayac, df_size, "%", remaining_per, remaining_time_mins))

            if sayac >= df_size:
                break


if __name__ == "__main__":
    # Boolean oprions parser
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
    ap.add_argument("-u", "--user", required=False, type=str, default="train",
                    help="Default: train")
    ap.add_argument("-p", "--password", required=False, type=str, default="Ankara06",
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
    ap.add_argument("-k", "--key_index", required=False, type=int, default=1000,
                    help="Which column will be send as key to kafka? If not used this option, pandas dataframe index will "
                         "be send. Default: 1000 indicates pandas index will be used.")

    args = vars(ap.parse_args())

    df_to_postgresql = DataFrameToPostgreSQL(
        input=args['input'],
        sep=args['sep'],
        row_sleep_time=args['row_sleep_time'],
        source_file_extension=args['source_file_extension'],
        repeat=args['repeat'],
        shuffle=args['shuffle'],
        key_index=args['key_index'],
        host=args['host'],
        user=args['user'],
        database=args['database'],
        password=args['password'],
        table=args['table']
    )
    df_to_postgresql.df_to_postgresql()
