# Genel
Streaming uygulamaları geliştiricileri için hazırlanmıştır. Heryerde bir çok veri setini kolaylıkla bularak batch geliştirmeler yapmak mümkün ancak aynısını streaming için söylemek mümkün değil. Yukarıdaki 2 farklı python scripti bu ihtiyacı karşılamak üzere hazırlanmıştır. Bir veri dosyasını okuyarak onu başka bir klasöre veya Kafka'ya stream olarak üretir. Güzel yönü stream üretim hızının ve log dosya büyüklüğünün ayarlanabiliyor olmasıdır. **Python3** kullanmalısınız. Virtual environment kullanmanız tavsiye olunur.

## kurulum
`pip install -r requirements.txt`

# DataframeToLog (dataframe_to_log.py)
Bir veri dosyasını(csv,txt,parquet) okuyarak onu belirlenen hızda hedef dizine belirlenen büyüklüklerde log olarak yazar.  
 
## 1. pandas ve pyarrow paketleri kurulu olmalıdır.
>python -m pip install pandas --user  
>python -m pip install pyarrow --user  

## 2. csv ve parquet dosyaları okuyabilir. Csv formatında yazabilir.

## 3. Log tazındaki çıktıları virgülle ayrılmış olarak üretir.

## 4. İlk başa ID, en sona da event_time adında iki sütun ilave ederek log üretir. (Eğer --output_index True ise)

## Örnek komut-1 Windows-csv:
csv dosya kaynağı için:  
>python dataframe_to_log.py --sep "," --input "input/iris.csv" --output "output" --batch_interval 0.1 --batch_size 10 --source_file_extension "csv" --prefix "iris_" --output_header False --output_index True

Beklenen komut satırı çıktısı
20/150 processed, % 86.67 will be completed in 0.43 mins.  
40/150 processed, % 73.33 will be completed in 0.37 mins.  
60/150 processed, % 60.00 will be completed in 0.30 mins.  

Yukarıdaki komut iris.csv dosyasını okur, 20 satırda bir ayrı dosya oluşturarak logs klasörü içine yazar.  

## Örnek komut-2 Windows-parquet:
>python dataframe_to_log.py --sep "," --input "input/flights_parquet" --output "input/iris_parquet/iris.snappy.parquet" --batch_interval 0.2 --batch_size 20 --source_file_extension "parquet" --prefix "sales_"  --output_header False --output_index True


## Örnek komut-3 linux-csv:
>python3.6 dataframe_to_log.py --sep "," --input "input/iris.csv" --output "output" --batch_interval 0.2 --batch_size 20 --source_file_extension "csv" --prefix "iris_"  --output_header False --output_index True --output_header False --output_index True

Komut çalıştıktan sonra hedef dizinde beklenen sonuç  
ll output/  
total 32  
-rw-rw-r-- 1 user user 1226 Jun 11 17:28 iris_20200611-172835  
-rw-rw-r-- 1 user user 1236 Jun 11 17:28 iris_20200611-172839  
-rw-rw-r-- 1 user user 1276 Jun 11 17:28 iris_20200611-172843  
-rw-rw-r-- 1 user user 1316 Jun 11 17:28 iris_20200611-172847  
-rw-rw-r-- 1 user user 1316 Jun 11 17:28 iris_20200611-172851  
-rw-rw-r-- 1 user user 1316 Jun 11 17:28 iris_20200611-172855  
-rw-rw-r-- 1 user user 1316 Jun 11 17:28 iris_20200611-172859  
-rw-rw-r-- 1 user user  696 Jun 11 17:29 iris_20200611-172901  



# DatafameToKafka (dataframe_to_kafka.py)
Bir veri dosyasını okuyarak onu belirlenen hızda Kafka'ya mesaj olarak gönderir.
Boş değerlerin olduğu satırları düşürür.
Dataframe repeat değeri kadar generate edilebilir. Örneğin repeat değeri 3 ise ve dataframe 
içinde 150 satır varsa toplam 450 satır üretilir. Satırların sırası karıştırlmak istenirse --shuffle=True yapılmalıdır.

## 1. pandas, pyarrow ve kafka-python paketi kurulu olmalıdır.
>python -m pip install pandas pyarrow kafka-python --user  

## 2. csv ve parquet dosyalarını okuyabilir.

## 2. Örnek komut
>python dataframe_to_kafka.py --input "D:\Datasets\LifeExpectancyData.csv" --sep="," --row_sleep_time 2 --source_file_extension="csv" --topic="deneme" --bootstrap_servers=["locahost:9092"]  --repeat 3 --shuffle True