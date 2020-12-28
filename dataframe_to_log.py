# coding: utf-8
import pandas as pd
import time, sys, os
from pathlib import Path
from datetime import datetime
import argparse
from random import randrange

"""
python dataframe_to_log.py --sep "," --input "input/iris.csv" --output "output" --batch_interval 0.1 --batch_size 10 --source_file_extension "csv" --prefix "iris_" --output_header False --output_index True
"""


class DataFrameDataGenerator:
    def __init__(self, input, output_folder, batch_interval, repeat, shuffle, batch_size, prefix,
                 sep, source_file_extension, output_header, output_index):
        self.sep = sep
        print("self.sep", self.sep)
        self.input = input
        print("self.input", self.input)
        self.df = self.read_source_file(source_file_extension)
        print("self.df", len(self.df))
        self.output_folder = output_folder
        print("self.output_folder", self.output_folder)
        self.batch_size = batch_size
        print("self.batch_size", self.batch_size)
        self.prefix = prefix
        print("self.prefix", self.prefix)
        self.batch_interval = batch_interval
        print("self.batch_interval", self.batch_interval)
        self.repeat = repeat
        print("self.repeat", self.repeat)
        self.shuffle = shuffle
        print("self.shuffle", self.shuffle)
        self.output_header = output_header
        print("self.output_header", self.output_header)
        self.output_index = output_index
        print("self.output_index", self.output_index)
        print("Starting in {} seconds... ".format(self.batch_interval * self.batch_size))


    def read_source_file(self, extension='csv'):
        if extension == 'csv':
            return pd.read_csv(self.input, sep=self.sep)
        else:
            return pd.read_parquet(self.input, 'auto')

    # write df to disk
    def df_to_file_as_log(self):
        # get dataframe size
        df_size = len(self.df)

        # calculate total streaming time
        total_time = self.batch_interval * df_size * self.repeat

        time_list_for_each_batch = []
        repeat_counter = 1
        total_counter = 1
        # loop for each row
        for j in range(0, self.repeat):
            i = 0
            sayac = 0
            for i in range(1, df_size + 1):

                # sleep for each row specified in batch_interval argument
                time.sleep(self.batch_interval)
                dateTimeObj = datetime.now()

                # collect timestamps for each row
                time_list_for_each_batch.append(dateTimeObj)

                # if row number mode batch size equals zero write those rows into disk.
                # do the same if row size equals to dataframe size.
                if (i % self.batch_size == 0) or (i == df_size):
                    # get this batch of dataframe to another one
                    df_batch = self.df.iloc[sayac:i, :].copy()

                    timestr = time.strftime("%Y%m%d-%H%M%S")

                    # add timestaps that are unique for each row
                    df_batch['event_time'] = time_list_for_each_batch

                    # Empty timestamp list because it must be fill for the next batch
                    time_list_for_each_batch = []

                    df_batch.to_csv(self.output_folder + "/" + self.prefix + str(timestr), header=self.output_header,
                                    index=self.output_index, index_label='ID', encoding='utf-8')
                    sayac = i
                    remaining_per = 100 - (100 * (total_counter / (self.repeat * df_size)))
                    remaining_time_secs = (total_time - (self.batch_interval * i * repeat_counter))
                    remaining_time_mins = remaining_time_secs / 60
                    print("%d/%d processed, %s %.2f will be completed in %.2f mins." % (
                        total_counter, df_size * self.repeat, "%", remaining_per, remaining_time_mins))
                    print("repeat_counter", repeat_counter)
                    print("total_counter", total_counter)
                total_counter += 1
            repeat_counter = repeat_counter + 1


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
    ap.add_argument("-s", "--sep", required=False, type=str, default=',',
                    help="seperatör giriniz. Varsayılan ,")
    ap.add_argument("-i", "--input", required=False, type=str, default='input/iris.csv',
                    help="Veri setine ait path giriniz. Varsayılan input/iris.csv")
    ap.add_argument("-o", "--output", required=False, type=str, default='output',
                    help="Hedef klasör.  Varsayılan output")
    ap.add_argument("-b", "--batch_interval", required=False, type=float, default=0.5,
                    help="Her satırda kaç saniye beklesin float. Varsayılan 0.5 saniye")
    ap.add_argument("-z", "--batch_size", required=False, type=int, default=10,
                    help="Kaç satırda bir dosya oluşturup hedefe yazsın int. Varsayılan 10 satır")
    ap.add_argument("-e", "--source_file_extension", required=False, type=str, default='csv',
                    help="Kaynak dosyanın uzantısı nedir?.Varsayılan csv")
    ap.add_argument("-x", "--prefix", required=False, type=str, default='my_df_',
                    help="Hedefe yazılacak dosya isminin ön eki. Varsayılan my_df_")
    ap.add_argument("-oh", "--output_header", required=False, type=str2bool, default=False,
                    help="Yazarken sütun isimleri/başlık olsun mu?. Varsayılan False")
    ap.add_argument("-idx", "--output_index", required=False, type=str2bool, default=False,
                    help="Yazarken satır indeksi eklensin mi?. Varsayılan False, yani eklenmesin")
    ap.add_argument("-r", "--repeat", required=False, type=int, default=1,
                    help="Dataframe'in kaç tur/tekrar generate edileceği. Varsayılan 1")
    ap.add_argument("-shf", "--shuffle", required=False, type=str2bool, default=False,
                    help="Dataframe'in satırları shuflle edilsin mi?. Varsayılan False, edilmesin")

    args = vars(ap.parse_args())

    df_log_generator = DataFrameDataGenerator(
        input=args['input'],
        output_folder=args['output'],
        batch_interval=args['batch_interval'],
        batch_size=args['batch_size'],
        prefix=args['prefix'],
        sep=args['sep'],
        source_file_extension=args['source_file_extension'],
        output_header=args['output_header'],
        output_index=args['output_index'],
        repeat=args['repeat'],
        shuffle=args['shuffle']
    )
    df_log_generator.df_to_file_as_log()
