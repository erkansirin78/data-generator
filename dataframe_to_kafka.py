# coding: utf-8
import pandas as pd
from kafka import KafkaProducer
import time, sys, os
from pathlib import Path
from datetime import datetime
import argparse
"""
Example:
python dataframe_to_kafka.py -i "D:/Datasets/iris.csv" -s "," -rst 0.5 -e "csv" -t iris -b ["cloudera:9092"] -r 3 -shf "True"
"""

class DataFrameToKafka:

    def __init__(self, input,  sep, row_sleep_time, source_file_extension, bootstrap_servers,
                 topic, repeat, shuffle):
        self.input = input
        self.sep = sep
        self.row_sleep_time = row_sleep_time
        self.repeat = repeat
        self.shuffle = shuffle
        self.df = self.read_source_file(source_file_extension)
        self.topic = topic
        try:
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        except:
            print("No Broker available")

    # puts all columns into one as string
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
        else:
            if self.shuffle is "True":
                df = pd.read_parquet(self.input, 'auto').sample(frac=1)
            else:
                df = pd.read_parquet(self.input, 'auto')
            df = df.dropna()
            df['value'] = self.turn_df_to_str(df)
            return df

    # bir dataframe'i kafka
    def df_to_kafka(self):

        sayac = 0
        repeat_counter = 0
        df_size = len(self.df) * self.repeat
        total_time = self.row_sleep_time * df_size
        for i in range(0, self.repeat):
            for index, row in self.df.iterrows():
                self.producer.send(self.topic, key=str(index).encode(), value=row[-1].encode())
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
                    help="Veri setine ait path giriniz. Varsayılan input/iris.csv")
    ap.add_argument("-s", "--sep", required=False, type=str, default=",",
                    help="seperatör giriniz. Varsayılan ,")
    ap.add_argument("-rst", "--row_sleep_time", required=False, type=float, default=0.5,
                    help="Her bir satır için bekleme süresi. Varsayılan 0.5")
    ap.add_argument("-e", "--source_file_extension", required=False, type=str, default="csv",
                    help="Kaynak veri setinin uzantısı nedir?. Varsayılan csv")
    ap.add_argument("-t", "--topic", required=False, type=str, default="test1",
                    help="Kafka topic. Varsayılan test1")
    ap.add_argument("-b", "--bootstrap_servers", required=False, type=list, default=["localhost:9092"],
                    help="kafka bootstraop servers ve portu  python listesi içinde. Varsayılan [localhost:9092]")
    ap.add_argument("-r", "--repeat", required=False, type=int, default=1,
                    help="Dataframe'in kaç tur generate edileceği. Varsayılan 1")
    ap.add_argument("-shf", "--shuffle", required=False, type=str2bool, default=False,
                    help="Dataframe'in satırları shuflle edilsin mi?. Varsayılan False")

    args = vars(ap.parse_args())

    df_to_kafka = DataFrameToKafka(
        input=args['input'],
        sep=args['sep'],
        row_sleep_time=args['row_sleep_time'],
        source_file_extension=args['source_file_extension'],
        topic=args['topic'],
        bootstrap_servers=args['bootstrap_servers'],
        repeat=args['repeat'],
        shuffle=args['shuffle']
    )
    df_to_kafka.df_to_kafka()
