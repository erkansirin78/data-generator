# coding: utf-8
import pandas as pd
import time
from datetime import datetime
import argparse

"""
python dataframe_to_log.py --sep "," \
--input "input/iris.csv" --output "output" \
--batch_interval 0.1 --batch_size 10 --source_file_extension "csv" \
--prefix "iris_" --output_header True --output_index True \
--log_sep '|' --excluded_cols 'Species' 'PetalWidthCm'
"""


class DataFrameDataGenerator:
    def __init__(self, input, output_folder, batch_interval, repeat, shuffle, batch_size, prefix,
                 sep, log_sep, source_file_extension, output_header, is_output_format_parquet, output_index,
                 excluded_cols):
        self.sep = sep
        print("self.sep", self.sep)
        self.log_sep = log_sep
        print("self.log_sep", self.log_sep)
        self.input = input
        print("self.input", self.input)
        self.excluded_cols = excluded_cols
        print("self.excluded_cols", self.excluded_cols)
        self.shuffle = shuffle
        print("self.shuffle", self.shuffle)
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
        self.output_header = output_header
        print("self.output_header", self.output_header)
        self.is_output_format_parquet = is_output_format_parquet
        print("self.is_output_format_parquet", self.is_output_format_parquet)
        self.output_index = output_index
        print("self.output_index", self.output_index)
        print("Starting in {} seconds... ".format(self.batch_interval * self.batch_size))

    def read_source_file(self, extension='csv'):
        if extension == 'csv':
            if self.shuffle is True:
                df = pd.read_csv(self.input, sep=self.sep).sample(frac=1)
                columns_to_write = [x for x in df.columns if x not in self.excluded_cols]
                print("columns_to_write", columns_to_write)
                df = df[columns_to_write]
            else:
                df = pd.read_csv(self.input, sep=self.sep)
                columns_to_write = [x for x in df.columns if x not in self.excluded_cols]
                print("columns_to_write", columns_to_write)
                df = df[columns_to_write]

            # Tüm sütun isimlerini küçük harfe dönüştürmek
            df.columns = df.columns.str.lower()

            # datetime sütunu zaten varsa
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
            else:
                # date ve time sütunları varsa birleştir yeni datetime sütunu oluştur
                if 'date' in df.columns and 'time' in df.columns:
                    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
                    # df.drop(['date', 'time'], axis=1, inplace=True)
            

            return df
        # if not csv, parquet
        else:
            if self.shuffle is True:
                df = pd.read_parquet(self.input, 'auto').sample(frac=1)
                columns_to_write = [x for x in df.columns if x not in self.excluded_cols]
                print("columns_to_write", columns_to_write)
                df = df[columns_to_write]
            else:
                df = pd.read_parquet(self.input, 'auto')
                columns_to_write = [x for x in df.columns if x not in self.excluded_cols]
                print("columns_to_write", columns_to_write)
                df = df[columns_to_write]

            df.columns = df.columns.str.lower()

            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
            else:
                if 'date' in df.columns and 'time' in df.columns:
                    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
                    # df.drop(['date', 'time'], axis=1, inplace=True)
            
            return df

    # write df to disk
    def df_to_file_as_log(self):

        # get dataframe size
        df_size = len(self.df)

        # calculate total streaming time
        total_time = self.batch_interval * df_size * self.repeat
    

        
        self.df.sort_values(by='datetime', inplace=True)
        
        print(self.df.head())


        self.df['sleep_duration'] = 0  # Sıfır ile başlat

        prev_datetime = None  # Önceki datetime ı tutmak için
        for index, row in self.df.iterrows():
            current_datetime = row['datetime']
            if prev_datetime is not None:
                print(prev_datetime)
                print(current_datetime)
            # Şu anki datetime ile önceki datetime arasındaki zaman farkını saniye 
                time_difference = (current_datetime - prev_datetime).total_seconds()
                print(time_difference)
            # Bu zaman farkı kadar bekliyoruz
                time.sleep(time_difference)
            # Verideki sleep_duration sütununu güncelleyip beklenen süreyi kayıt
                self.df.at[index, 'sleep_duration'] = time_difference
        # Önceki datetime i güncelle bir sonraki için
            prev_datetime = current_datetime


        # Bekleme sürelerini saklamak için sleep_duration sütununu ekleme
        
        

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

                    if self.is_output_format_parquet:
                        df_batch.to_parquet(self.output_folder + "/" + self.prefix + str(timestr) + ".parquet",
                                            engine='pyarrow', index=self.output_index)
                    else:
                        df_batch.to_csv(self.output_folder + "/" + self.prefix + str(timestr),
                                        header=self.output_header,
                                        index=self.output_index, index_label='ID', encoding='utf-8', sep=self.log_sep)

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
                    help="Delimiter. Default: ,")
    ap.add_argument("-ls", "--log_sep", required=False, type=str, default=',',
                    help="In log file how the fields should separated. Default ,")
    ap.add_argument("-i", "--input", required=False, type=str, default='input/iris.csv',
                    help="Input data path. Default: ./input/iris.csv")
    ap.add_argument("-o", "--output", required=False, type=str, default='output',
                    help="Output folder. It must be exists.  Default ./output")
    ap.add_argument("-b", "--batch_interval", required=False, type=float, default=0.5,
                    help="Time to sleep for every row. Default 0.5 seconds")
    ap.add_argument("-z", "--batch_size", required=False, type=int, default=10,
                    help="How many rows should be in a single log file. Default 10 rows")
    ap.add_argument("-e", "--source_file_extension", required=False, type=str, default='csv',
                    help="File extension of source file. If specified other than csv it is considered parquet. Default csv")
    ap.add_argument("-x", "--prefix", required=False, type=str, default='my_log_',
                    help="The prefix of log filename. Default my_log_")
    ap.add_argument("-oh", "--output_header", required=False, type=str2bool, default=True,
                    help="Should log files have header?. Default True")
    ap.add_argument("-ofp", "--is_output_format_parquet", required=False, type=str2bool, default=False,
                    help="Is output format be parquet? If True will write parquet format. Default False")
    ap.add_argument("-idx", "--output_index", required=False, type=str2bool, default=False,
                    help="Should log file have index field. Default False, no index")
    ap.add_argument("-r", "--repeat", required=False, type=int, default=1,
                    help="Round number that how many times dataset generated. Default 1")
    ap.add_argument("-shf", "--shuffle", required=False, type=str2bool, default=False,
                    help="Should dataset shuffled before to generate log?. Default False, no shuffle")
    ap.add_argument("-exc", "--excluded_cols", required=False, nargs='+', default=['it_is_impossible_column'],
                    help="The columns not to write log file?. Default ['it_is_impossible_column']. Ex: -exc 'Species' 'PetalWidthCm'")

    args = vars(ap.parse_args())

    df_log_generator = DataFrameDataGenerator(
        input=args['input'],
        output_folder=args['output'],
        batch_interval=args['batch_interval'],
        batch_size=args['batch_size'],
        prefix=args['prefix'],
        sep=args['sep'],
        log_sep=args['log_sep'],
        source_file_extension=args['source_file_extension'],
        output_header=args['output_header'],
        is_output_format_parquet=args['is_output_format_parquet'],
        output_index=args['output_index'],
        repeat=args['repeat'],
        shuffle=args['shuffle'],
        excluded_cols=args['excluded_cols']
    )
    df_log_generator.df_to_file_as_log()


