# Intro
It is easy to find data sources for batch processing, but it is hard to tell same for realtime processing.
This repo aims to make easy realtime data processing developments by streaming static datasets to file or Apache Kafka.
There are 2 python scripts: one for stream data to file (`dataframe_to_log.py`) and the other to Kafka (`dataframe_to_kafka.py`).
You must use ** Python3 **. It is recommended to use virtual environment.
# Installation
```
erkan@ubuntu:~$ git clone https://github.com/erkansirin78/data-generator.git

erkan@ubuntu:~$ cd data-generator/

erkan@ubuntu:~/data-generator$ python3 -m virtualenv datagen

erkan@ubuntu:~/data-generator$ source datagen/bin/activate
(datagen) erkan@ubuntu:~/data-generator$ python -V
Python 3.6.9

(datagen) erkan@ubuntu:~/data-generator$ pip install -r requirements.txt
```