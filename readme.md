# Genel
Streaming uygulamaları geliştiricileri için hazırlanmıştır. Heryerde bir çok veri setini kolaylıkla bularak batch 
geliştirmeler yapmak mümkün ancak aynısını streaming için söylemek mümkün değil. 
Yukarıdaki 2 farklı python scripti bu ihtiyacı karşılamak üzere hazırlanmıştır. 
Bir veri dosyasını okuyarak onu başka bir klasöre (`dataframe_to_log.py`) veya Kafka'ya (`dataframe_to_kafka.py`) 
stream olarak üretir. Güzel yönü stream üretim hızının ve log dosya büyüklüğünün ayarlanabiliyor olmasıdır. **Python3** 
kullanmalısınız. Virtual environment kullanmanız tavsiye olunur.

# Intro
It is prepared for streaming applications developers. It is possible to make batch improvements by finding many data sets everywhere easily, but it is not possible to say the same for streaming.
The above 2 different python scripts have been prepared to meet this need.
Reading a data file and sending it to another folder `dataframe_to_log.py`) or Kafka (` dataframe_to_kafka.py`)
It produces as a stream. The nice thing is that stream generation speed and log file size can be adjusted.
  You must use ** Python3 **. It is recommended to use virtual environment. 

# DataframeToLog (dataframe_to_log.py)
## TR
- Bir veri dosyasını(csv,txt,parquet) okuyarak onu belirlenen hızda hedef dizine belirlenen büyüklüklerde log olarak yazar.  
- pandas ve pyarrow paketleri kurulu olmalıdır.

`python -m pip install pandas --user`

`python -m pip install pyarrow --user`

- csv ve parquet dosyaları okuyabilir. Csv formatında yazabilir.

- Log tazındaki çıktıları virgülle ayrılmış olarak üretir.

- İlk başa ID, en sona da event_time adında iki sütun ilave ederek log üretir. (Eğer --output_index True ise)

- Örnek komut-1 Windows-csv:
csv dosya kaynağı için:  
```
python dataframe_to_log.py --sep "," \
--input "input/iris.csv" \
--output "output" \
--batch_interval 0.1 \
--batch_size 10 \
--source_file_extension "csv" \
--prefix "iris_" \
--output_header False \
--output_index True
```
Beklenen komut satırı çıktısı
```
20/150 processed, % 86.67 will be completed in 0.43 mins.  
40/150 processed, % 73.33 will be completed in 0.37 mins.  
60/150 processed, % 60.00 will be completed in 0.30 mins.  
```
Yukarıdaki komut iris.csv dosyasını okur, 20 satırda bir ayrı dosya oluşturarak logs klasörü içine yazar.  

- Örnek parquet:
```
python dataframe_to_log.py \
--sep "," \
--input "input/flights_parquet" \
--output "output/iris_parquet/iris.snappy.parquet" \
--batch_interval 0.2 \
--batch_size 20 \
--source_file_extension "parquet" \
--prefix "sales_"  \
--output_header False \
--output_index True
```

## ENG
- It reads a data file (csv, txt, parquet) and writes it to the target directory at the specified speed as a log in the specified sizes.
- pandas and payarrow must be installed


`python -m pip install pandas --user`

`python -m pip install pyarrow --user`

- Can read csv and parquet files and write in csv format.

- It produces outputs in log type separated by commas.

- It generates a log by adding two columns named ID at the beginning and event_time at the end. (If --output_index is True)

- Sample for csv:
csv source:  
```
python dataframe_to_log.py --sep "," \
--input "input/iris.csv" \
--output "output" \
--batch_interval 0.1 \
--batch_size 10 \
--source_file_extension "csv" \
--prefix "iris_" \
--output_header False \
--output_index True
```
Expected stdout on commandline
```
20/150 processed, % 86.67 will be completed in 0.43 mins.  
40/150 processed, % 73.33 will be completed in 0.37 mins.  
60/150 processed, % 60.00 will be completed in 0.30 mins.  
```
The above command reads the iris.csv file, creates a separate file in 20 lines and writes it into the logs folder. 

-Sample for parquet:
```
python dataframe_to_log.py \
--sep "," \
--input "input/flights_parquet" \
--output "output/iris_parquet/iris.snappy.parquet" \
--batch_interval 0.2 \
--batch_size 20 \
--source_file_extension "parquet" \
--prefix "sales_"  \
--output_header False \
--output_index True
```

# Kafka DatafameToKafka (dataframe_to_kafka.py)
## TR
### Kurulum

`git clone https://github.com/erkansirin78/data-generator.git`

` cd data-generator `

`python3 -m virtualenv kafka `

`source kafka/bin/activate `

`pip install -r requirements.txt `
                        
`python dataframe_to_kafka.py `


### Açıklama
Bir veri dosyasını okuyarak onu belirlenen hızda Kafka'ya mesaj olarak gönderir.
Boş değerlerin olduğu satırları düşürür.
Dataframe repeat değeri kadar generate edilebilir. Örneğin repeat değeri 3 ise ve dataframe 
içinde 150 satır varsa toplam 450 satır üretilir. 
Satırların sırası karıştırlmak istenirse --shuffle=True yapılmalıdır.
Herhangi bir sütun Kafka'ya key olarak gönderilmek istenirse -k veya --key_index option ile sütun indeksi belirtilmelidir.
Örneğin ilk sütun ise 0, İkinci sütun ise 1. Bu seçenek kullanılmadığında varsayılan indeks 1000 gider ve bu da pandas dataframe indeksini key olarak gönderir.
Yani indeks 1000 rezerve edilmiştir ve her halükarda Kafka'ya key gönderilir.

### Manuel kurulum
### pandas, pyarrow ve kafka-python paketi kurulu olmalıdır.
`python -m pip install pandas pyarrow kafka-python --user`

### csv ve parquet dosyalarını okuyabilir.

### Örnek komut
```
python dataframe_to_kafka.py \
--input "D:\Datasets\LifeExpectancyData.csv" \
--sep="," \
--row_sleep_time 2 \
--source_file_extension="csv" \
--topic="deneme" \
--bootstrap_servers=["locahost:9092"]  \
--repeat 3 \
--shuffle True
--key_index 2
```

## ENG

### Install the packages with virtualev
`git clone https://github.com/erkansirin78/data-generator.git`

` cd data-generator `  

`python3 -m virtualenv kafka `

`source kafka/bin/activate `

`pip install -r requirements.txt `
                        
`python dataframe_to_kafka.py `


### Intro
It reads a data file and sends it as a message to Kafka at the specified speed.
Drops rows with empty values.
Dataframe can be generated as much as the repeat value. For example, if the repeat is 3 and there are 150 lines in the dataframe, a total of 450 lines will be generated.
If you want to shuffle the order of the lines, --shuffle = True.
If you want to send any column as key to Kafka, the column index must be specified with -k or --key_index option.
For example, if the first column is 0, the second column is 1. If this option is not used, the default index goes to 1000 and this sends the pandas dataframe index as key.
So index 1000 is reserved and in any case the key is sent to Kafka.

### Manuel installation
### pandas, pyarrow ve kafka-python packages must be installed.
`python -m pip install pandas pyarrow kafka-python --user`

### Can read csv and parquet files.

### Sample command
```
python dataframe_to_kafka.py \
--input "D:\Datasets\LifeExpectancyData.csv" \
--sep="," \
--row_sleep_time 2 \
--source_file_extension="csv" \
--topic="deneme" \
--bootstrap_servers=["locahost:9092"]  \
--repeat 3 \
--shuffle True
--key_index 2
```