# Intro
It is easy to find data sources for batch processing, but it is hard to tell same for realtime processing.
This repo aims to make easy realtime data processing developments by streaming static datasets to file, postgresql, and Apache Kafka.
There are 3 python scripts:
- Stream data to file (`dataframe_to_log.py`) as log files
- PostgreSQL (`dataframe_to_postgresql.py`)
- Kafka (`dataframe_to_kafka.py`)

You must use ** Python3 **.

# Installation
```
erkan@ubuntu:~$ git clone https://github.com/erkansirin78/data-generator.git

erkan@ubuntu:~$ cd data-generator/

erkan@ubuntu:~/data-generator$ python3 -m virtualenv datagen

erkan@ubuntu:~/data-generator$ source datagen/bin/activate

(datagen) erkan@ubuntu:~/data-generator$ pip install -r requirements.txt
```