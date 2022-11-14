# Intro
It is easy to find data sources for batch processing, but it is hard to tell same for realtime processing.
This repo aims to make easy realtime data processing developments by streaming static datasets to file, postgresql, Apache Kafka, AWS S3/MinIO.
There are 4 python scripts:
- Stream data to file (`dataframe_to_log.py`) as log files
- PostgreSQL (`dataframe_to_postgresql.py`)
- Kafka (`dataframe_to_kafka.py`)
- AWS S3 (`dataframe_to_s3.py`)

You must use ** Python3 **.

# Installation
```
erkan@ubuntu:~$ git clone https://github.com/erkansirin78/data-generator.git

erkan@ubuntu:~$ python3 -m pip install virtualenv

erkan@ubuntu:~$ cd data-generator/

erkan@ubuntu:~/data-generator$ python3 -m virtualenv datagen

erkan@ubuntu:~/data-generator$ source datagen/bin/activate

(datagen) erkan@ubuntu:~/data-generator$ pip install -r requirements.txt
```
