# coding: utf-8
import pandas as pd
from kafka import KafkaProducer
import time, sys, os
from pathlib import Path
from datetime import datetime
import argparse
"""
Example:
python dataframe_to_kafka.py -i "D:/Datasets/iris.csv" -s "," -rst 0.5 -e "csv" -t iris -b "kafka_broker1:9092" "kafka_broker2:9092" -r 3 -shf "True"
"""

class DataFrameToKafka:

    def __init__(self, input,  sep, row_sleep_time, source_file_extension, bootstrap_servers,
                 topic, repeat, shuffle, key_index):
        self.input = input
        print("input: {}".format(self.input))
        self.sep = sep
        print("sep: {}".format(self.sep))
        self.row_sleep_time = row_sleep_time
        print("row_sleep_time: {}".format(self.row_sleep_time))
        self.repeat = repeat
        print("repeat: {}".format(self.repeat))
        self.shuffle = shuffle
        print("shuffle: {}".format(self.shuffle))
        self.df = self.read_source_file(source_file_extension)
        self.topic = topic
        print("topic: {}".format(self.topic))
        self.key_index = key_index
        print("key_index: {}".format(self.key_index))
        print("bootstrap_servers: {}".format(bootstrap_servers))
        try:
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        except:
            print("No Broker available")

    # puts all columns into one as string. Index -1 will be all columns
    def turn_df_to_str(self, df):
        x = df.to_string(header=False,
                         index=False,
                         index_names=False).split('\n')

        vals = [','.join(ele.split()) for ele in x]
        return vals

    def read_source_file(self, extension='csv'):
        if extension == 'csv':
            if self.shuffle == "True":
                df = pd.read_csv(self.input, sep=self.sep).sample(frac=1)
            else:
                df = pd.read_csv(self.input, sep=self.sep)
            df = df.dropna()

            # put all cols into value column
            df['value'] = self.turn_df_to_str(df)
            return df
        # if not csv, parquet
        else:
            if self.shuffle is "True":
                df = pd.read_parquet(self.input, 'auto').sample(frac=1)
            else:
                df = pd.read_parquet(self.input, 'auto')
            df = df.dropna()
            # put all cols into value column
            df['value'] = self.turn_df_to_str(df)
            return df

    # Produce a pandas dataframe to kafka
    def df_to_kafka(self):

        sayac = 0
        repeat_counter = 0
        df_size = len(self.df) * self.repeat
        total_time = self.row_sleep_time * df_size
        for i in range(0, self.repeat):
            for index, row in self.df.iterrows():

                if self.key_index == 1000:
                    self.producer.send(self.topic, key=str(index).encode(), value=row[-1].encode())
                    # row[-1] corresponds to all columns which already put in one colum named value
                    # If  -k or --key_index not used pandas df index will be sent to kafka as key
                else:
                    self.producer.send(self.topic, key=str(row[self.key_index]).encode(), value=row[-1].encode())
                    # if -k or --key_index used the column spesified in this option will be sent to kafka as key
                self.producer.flush()
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
        self.producer.close()


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
                    help="Data path. Default: ./input/iris.csv")
    ap.add_argument("-s", "--sep", required=False, type=str, default=",",
                    help="Delimiter. Default: ,")
    ap.add_argument("-rst", "--row_sleep_time", required=False, type=float, default=0.5,
                    help="Sleep time in seconds per row. Default: 0.5")
    ap.add_argument("-e", "--source_file_extension", required=False, type=str, default="csv",
                    help="Extension of data file. Default: csv")
    ap.add_argument("-t", "--topic", required=False, type=str, default="test1",
                    help="Kafka topic. Default: test1")
    ap.add_argument("-b", "--bootstrap_servers", required=False, nargs='+', default=["localhost:9092"],
                    help="Kafka bootstrap servers and port in a python list. Default: [localhost:9092]")
    ap.add_argument("-r", "--repeat", required=False, type=int, default=1,
                    help="How many times to repeat dataset. Default: 1")
    ap.add_argument("-shf", "--shuffle", required=False, type=str2bool, default=False,
                    help="Shuffle the rows?. Default: False")
    ap.add_argument("-k", "--key_index", required=False, type=int, default=1000,
                    help="Which column will be send as key to kafka? If not used this option, pandas dataframe index will "
                         "be send. Default: 1000 indicates pandas index will be used.")

    args = vars(ap.parse_args())

    df_to_kafka = DataFrameToKafka(
        input=args['input'],
        sep=args['sep'],
        row_sleep_time=args['row_sleep_time'],
        source_file_extension=args['source_file_extension'],
        topic=args['topic'],
        bootstrap_servers=args['bootstrap_servers'],
        repeat=args['repeat'],
        shuffle=args['shuffle'],
        key_index=args['key_index']
    )
    df_to_kafka.df_to_kafka()
