# coding: utf-8
import pandas as pd
import time, io
from datetime import datetime
import argparse
import logging
import boto3
from botocore.config import Config

"""
 python dataframe_to_s3.py -i ~/datasets/Mall_Customers.csv \
 -buc dataops -k "mydata/mc" -aki root -sac root12345 \
 -eu http://localhost:9000 -ofp True
"""


class DataFrameDataGenerator:
    def __init__(self, input, bucket, key, access_key_id, secret_access_key, endpoint_url, batch_interval, repeat, shuffle, batch_size,
                 sep, log_sep, source_file_extension, output_header, is_output_format_parquet, output_index, excluded_cols):
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
        self.bucket = bucket
        print("self.bucket", self.bucket)
        self.key = key
        print("self.key", self.key)
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.endpoint_url = endpoint_url
        print("self.endpoint_url", self.endpoint_url)
        self.batch_size = batch_size
        print("self.batch_size", self.batch_size)
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
            return df

    def get_s3_client(self):
        s3_client = boto3.client('s3',
                                endpoint_url=self.endpoint_url,
                                aws_access_key_id=self.access_key_id,
                                aws_secret_access_key=self.secret_access_key,
                                config=Config(signature_version='s3v4'))
        return s3_client

    def save_df_to_s3(self, df, bucket, key, is_output_format_parquet=False, index=False, header=False):
        ''' Store df as a buffer, then save buffer to s3'''
        s3_client = self.get_s3_client()
        try:
            if is_output_format_parquet:
                buffer = io.BytesIO()
                df.to_parquet(buffer, engine='pyarrow', index=self.output_index)
                s3_client.put_object(Body=buffer.getvalue(), Bucket=bucket, Key=key)
                logging.info(f'{key} saved to s3 bucket {bucket}')
            else:
                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=self.output_header)
                s3_client.put_object(Body=buffer.getvalue(), Bucket=bucket, Key=key)
                logging.info(f'{key} saved to s3 bucket {bucket}')
        except Exception as e:
            raise logging.exception(e)

    # write df to disk
    def df_to_s3_as_log(self):
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

                    if self.is_output_format_parquet:
                        # df_batch.to_parquet(self.key + "/" + str(timestr) + ".parquet",
                        #                     engine='pyarrow', index=self.output_index)
                        key = self.key + "_" + str(timestr) + ".parquet"
                        self.save_df_to_s3(df=df_batch, bucket=self.bucket, key=key,
                                           is_output_format_parquet=self.is_output_format_parquet, index=False)
                    else:
                        # df_batch.to_csv(self.key + "/" + str(timestr),
                        #                 header=self.output_header,
                        #                 index=self.output_index, index_label='ID', encoding='utf-8', sep=self.log_sep)
                        key = self.key + "_" + str(timestr) + ".csv"
                        self.save_df_to_s3(df=df_batch, bucket=self.bucket, key=key,
                                           is_output_format_parquet=self.is_output_format_parquet, index=False)
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
    ap.add_argument("-buc", "--bucket", required=False, type=str, default='dataops',
                    help="Bucket. For S3 works as prefix. E.g. my-bucket-name/.  Default dataops")
    ap.add_argument("-k", "--key", required=False, type=str, default='my_data/my_log_',
                    help="""Key. The path and the file name of object in the bucket. 
                    Timestamp and file extension will add as suffix.  Default my_data/my_log_""")
    ap.add_argument("-aki", "--access_key_id", required=False, type=str, default='root',
                    help="access_key_id.  Default root")
    ap.add_argument("-sac", "--secret_access_key", required=False, type=str, default='root12345',
                    help="secret_access_key.  Default root12345")
    ap.add_argument("-eu", "--endpoint_url", required=False, type=str, default='http://localhost:9000',
                    help="For s3 https://<bucket-name>.s3.<region>.amazonaws.com.  Default for MinIO http://localhost:9000")
    ap.add_argument("-s", "--sep", required=False, type=str, default=',',
                    help="Delimiter. Default: ,")
    ap.add_argument("-ls", "--log_sep", required=False, type=str, default=',',
                    help="In log file how the fields should separated. Default ,")
    ap.add_argument("-i", "--input", required=False, type=str, default='input/iris.csv',
                    help="Input data path. Default: ./input/iris.csv")
    ap.add_argument("-b", "--batch_interval", required=False, type=float, default=0.1,
                    help="Time to sleep for every row. Default 0.1 seconds")
    ap.add_argument("-z", "--batch_size", required=False, type=int, default=50,
                    help="How many rows should be in a single log file. Default 50 rows")
    ap.add_argument("-e", "--source_file_extension", required=False, type=str, default='csv',
                    help="File extension of source file. If specified other than csv it is considered parquet. Default csv")
    ap.add_argument("-oh", "--output_header", required=False, type=str2bool, default=False,
                    help="Should log files have header?. Default False")
    ap.add_argument("-ofp", "--is_output_format_parquet", required=False, type=str2bool, default=False,
                    help="Is output format be parquet? If True will write parquet format. Default False")
    ap.add_argument("-idx", "--output_index", required=False, type=str2bool, default=False,
                    help="Should log file have index field. Default False, no index")
    ap.add_argument("-r", "--repeat", required=False, type=int, default=1,
                    help="Round number that how many times dataset generated. Default 1")
    ap.add_argument("-shf", "--shuffle", required=False, type=str2bool, default=False,
                    help="Should dataset shuffled before to generate log?. Default False, no shuffle")
    ap.add_argument("-exc", "--excluded_cols", required=False, nargs='+', default='it_is_impossible_column',
                    help="The columns not to write log file?. Default 'it_is_impossible_column'. E.g.: -exc 'Species' 'PetalWidthCm'")

    args = vars(ap.parse_args())

    df_log_generator = DataFrameDataGenerator(
        bucket=args['bucket'],
        key=args['key'],
        access_key_id=args['access_key_id'],
        secret_access_key=args['secret_access_key'],
        endpoint_url=args['endpoint_url'],
        input=args['input'],
        batch_interval=args['batch_interval'],
        batch_size=args['batch_size'],
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
    df_log_generator.df_to_s3_as_log()
