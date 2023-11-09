# PySpark-Assignments



pip install pyspark
     
Collecting pyspark
  Downloading pyspark-3.4.1.tar.gz (310.8 MB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 310.8/310.8 MB 4.7 MB/s eta 0:00:00
  Preparing metadata (setup.py) ... done
Requirement already satisfied: py4j==0.10.9.7 in /usr/local/lib/python3.10/dist-packages (from pyspark) (0.10.9.7)
Building wheels for collected packages: pyspark
  Building wheel for pyspark (setup.py) ... done
  Created wheel for pyspark: filename=pyspark-3.4.1-py2.py3-none-any.whl size=311285388 sha256=d3bc96e545a3236000911df29ad0f6852ebfd0f9ed45ae4efabb728c2ecf284c
  Stored in directory: /root/.cache/pip/wheels/0d/77/a3/ff2f74cc9ab41f8f594dabf0579c2a7c6de920d584206e0834
Successfully built pyspark
Installing collected packages: pyspark
Successfully installed pyspark-3.4.1

from pyspark.sql.session import *
from pyspark.context import *
from pyspark.sql.functions import *
sc=SparkContext.getOrCreate()
spark=SparkSession(sc)
     

#Reading the data
df1=spark.read.format("csv") \
  .option('header','True')\
  .option('delimiter',',')\
  .option('inferSchema','True')\
  .load('/content/drive/MyDrive/Spark Datasets/Case.csv')
df2=spark.read.format("csv") \
  .option('header','True')\
  .option('delimiter',',')\
  .option('inferSchema','True')\
  .load('/content/drive/MyDrive/Spark Datasets/Region.csv')
df3=spark.read.format("csv") \
  .option('header','True')\
  .option('delimiter',',')\
  .option('inferSchema','True')\
  .load('/content/drive/MyDrive/Spark Datasets/TimeProvince.csv')
#Showing it
df1.show(5)
df2.show(5)
df3.show(5)
#Counting the number of records
print(df1.count())
print(df2.count())
df3.count()
     
+--------+--------+------------+-----+--------------------+---------+---------+----------+
| case_id|province|        city|group|      infection_case|confirmed| latitude| longitude|
+--------+--------+------------+-----+--------------------+---------+---------+----------+
| 1000001|   Seoul|  Yongsan-gu| true|       Itaewon Clubs|      139|37.538621|126.992652|
| 1000002|   Seoul|   Gwanak-gu| true|             Richway|      119| 37.48208|126.901384|
| 1000003|   Seoul|     Guro-gu| true| Guro-gu Call Center|       95|37.508163|126.884387|
| 1000004|   Seoul|Yangcheon-gu| true|Yangcheon Table T...|       43|37.546061|126.874209|
| 1000005|   Seoul|   Dobong-gu| true|     Day Care Center|       43|37.679422|127.044374|
+--------+--------+------------+-----+--------------------+---------+---------+----------+
only showing top 5 rows

+-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
| code|province|       city| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
+-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|10000|   Seoul|      Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
|10010|   Seoul| Gangnam-gu|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|10020|   Seoul|Gangdong-gu|37.530492|127.123837|                     27|                32|               0|         1.54|                   14.55|                5.4|              1023|
|10030|   Seoul| Gangbuk-gu|37.639938|127.025508|                     14|                21|               0|         0.67|                   19.49|                8.5|               628|
|10040|   Seoul| Gangseo-gu|37.551166|126.849506|                     36|                56|               1|         1.17|                   14.39|                5.7|              1080|
+-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
only showing top 5 rows

+----------+----+--------+---------+--------+--------+
|      date|time|province|confirmed|released|deceased|
+----------+----+--------+---------+--------+--------+
|2020-01-20|  16|   Seoul|        0|       0|       0|
|2020-01-20|  16|   Busan|        0|       0|       0|
|2020-01-20|  16|   Daegu|        0|       0|       0|
|2020-01-20|  16| Incheon|        1|       0|       0|
|2020-01-20|  16| Gwangju|        0|       0|       0|
+----------+----+--------+---------+--------+--------+
only showing top 5 rows

174
244
2771

#describing the df.
print(df1.describe())
print(df2.describe())
df3.describe()
     
DataFrame[summary: string,  case_id: string, province: string, city: string, infection_case: string, confirmed: string, latitude: string, longitude: string]
DataFrame[summary: string, code: string, province: string, city: string, latitude: string, longitude: string, elementary_school_count: string, kindergarten_count: string, university_count: string, academy_ratio: string, elderly_population_ratio: string, elderly_alone_ratio: string, nursing_home_count: string]
DataFrame[summary: string, time: string, province: string, confirmed: string, released: string, deceased: string]

#printing the schema
print(df1.printSchema())
print(df2.printSchema())
print(df3.printSchema())
     
root
 |--  case_id: integer (nullable = true)
 |-- province: string (nullable = true)
 |-- city: string (nullable = true)
 |-- group: boolean (nullable = true)
 |-- infection_case: string (nullable = true)
 |-- confirmed: integer (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)

None
root
 |-- code: integer (nullable = true)
 |-- province: string (nullable = true)
 |-- city: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- elementary_school_count: integer (nullable = true)
 |-- kindergarten_count: integer (nullable = true)
 |-- university_count: integer (nullable = true)
 |-- academy_ratio: double (nullable = true)
 |-- elderly_population_ratio: double (nullable = true)
 |-- elderly_alone_ratio: double (nullable = true)
 |-- nursing_home_count: integer (nullable = true)

None
root
 |-- date: date (nullable = true)
 |-- time: integer (nullable = true)
 |-- province: string (nullable = true)
 |-- confirmed: integer (nullable = true)
 |-- released: integer (nullable = true)
 |-- deceased: integer (nullable = true)

None

#Checking if duplicate elements are present in df1
col1=df1.columns
counts=df1.groupBy(*col1).count()
counts.show()
dp=counts.filter(col('count')>1).count() > 0
if dp:
  print('duplicate records are pesent in df1')
else:
  print('no duplicate records are pesent in df1')

#df2
col2=df2.columns
counts=df2.groupBy(*col2).count()
counts.show()
dp=counts.filter(col('count')>1).count() > 0
if dp:
  print('duplicate records are pesent in df1')
else:
  print('no duplicate records are pesent in df1')

#df3
col3=df3.columns
counts=df3.groupBy(*col3).count()
counts.show()
dp=counts.filter(col('count')>1).count() > 0
if dp:
  print('duplicate records are pesent in df1')
else:
  print('no duplicate records are pesent in df1')
     
+--------+----------------+---------------+-----+--------------------+---------+---------+----------+-----+
| case_id|        province|           city|group|      infection_case|confirmed| latitude| longitude|count|
+--------+----------------+---------------+-----+--------------------+---------+---------+----------+-----+
| 1600004|           Ulsan|              -|false|                 etc|        7|        -|         -|    1|
| 2000007|     Gyeonggi-do|from other city| true|  Shincheonji Church|       29|        -|         -|    1|
| 6100004|Gyeongsangnam-do|   Geochang-gun| true|Geochang-gun Woon...|        8|35.805681|127.917805|    1|
| 1200009|           Daegu|              -|false|contact with patient|      917|        -|         -|    1|
| 5000004|    Jeollabuk-do|              -|false|     overseas inflow|       12|        -|         -|    1|
| 6100009|Gyeongsangnam-do|from other city| true|       Onchun Church|        2|        -|         -|    1|
| 1700003|          Sejong|from other city| true|  Shincheonji Church|        1|        -|         -|    1|
| 1100010|           Busan|              -|false|                 etc|       30|        -|         -|    1|
| 1000015|           Seoul|        Jung-gu| true|Jung-gu Fashion C...|        7|37.562405|126.984377|    1|
| 1000032|           Seoul|        Jung-gu| true|Seoul City Hall S...|        3|37.565699|126.977079|    1|
| 2000012|     Gyeonggi-do|       Suwon-si| true|Lotte Confectione...|       15|37.287356|127.013827|    1|
| 1500008|         Daejeon|              -|false|     overseas inflow|       15|        -|         -|    1|
| 1000009|           Seoul|from other city| true|Coupang Logistics...|       25|        -|         -|    1|
| 1400005|         Incheon|              -|false|     overseas inflow|       68|        -|         -|    1|
| 1400007|         Incheon|              -|false|                 etc|       11|        -|         -|    1|
| 6100005|Gyeongsangnam-do|    Changwon-si| true|Hanmaeum Changwon...|        7| 35.22115|  128.6866|    1|
| 1000002|           Seoul|      Gwanak-gu| true|             Richway|      119| 37.48208|126.901384|    1|
| 3000007|      Gangwon-do|              -|false|contact with patient|        0|        -|         -|    1|
| 2000004|     Gyeonggi-do|from other city| true|             Richway|       58|        -|         -|    1|
| 6100010|Gyeongsangnam-do|              -|false|     overseas inflow|       26|        -|         -|    1|
+--------+----------------+---------------+-----+--------------------+---------+---------+----------+-----+
only showing top 20 rows

no duplicate records are pesent in df1
+-----+-----------------+-------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+-----+
| code|         province|         city| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|count|
+-----+-----------------+-------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+-----+
|10000|            Seoul|        Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|    1|
|10100|            Seoul|    Dobong-gu|37.668952|127.047082|                     23|                26|               1|         0.95|                   17.89|                7.2|               485|    1|
|13020|          Gwangju|       Nam-gu|35.132982|126.902379|                     23|                44|               4|         2.63|                   16.76|                7.5|               427|    1|
|30160|       Gangwon-do|Hongcheon-gun|37.697037|127.888821|                     26|                26|               0|         1.14|                   25.28|               12.6|               115|    1|
|41080|Chungcheongnam-do|    Seosan-si|36.784751|126.450289|                     29|                39|               1|          1.3|                   18.01|                8.4|               261|    1|
|51190|     Jeollanam-do|    Jindo-gun|34.486834|126.263554|                     10|                13|               0|         0.78|                   32.97|               22.0|                68|    1|
|14060|          Incheon|  Bupyeong-gu|37.507031|126.721804|                     42|                66|               0|         1.27|                   13.83|                6.2|               847|    1|
|40030|Chungcheongbuk-do|    Boeun-gun|36.489402|127.729503|                     15|                16|               0|         0.67|                   33.46|               18.4|                74|    1|
|40110|Chungcheongbuk-do|   Chungju-si|36.991009|127.925946|                     37|                44|               2|         1.39|                   19.07|                9.0|               347|    1|
|14080|          Incheon|    Yeonsu-gu|37.410262|126.678309|                     23|                43|               1|         1.77|                    9.48|                4.0|               467|    1|
|30130|       Gangwon-do| Chuncheon-si|37.881281|127.730088|                     40|                43|               5|          1.5|                    17.1|                7.6|               472|    1|
|60170| Gyeongsangbuk-do|  Ulleung-gun|37.484438|130.905883|                      4|                 6|               0|         0.21|                   24.81|               10.7|                11|    1|
|61180| Gyeongsangnam-do| Hapcheon-gun|35.566702| 128.16587|                     17|                15|               0|         0.71|                   38.44|               24.7|                96|    1|
|11060|            Busan|   Dongnae-gu| 35.20506|129.083673|                     22|                31|               0|         1.98|                   17.53|                7.7|               608|    1|
|14050|          Incheon|      Dong-gu|37.474101|126.643266|                      8|                12|               1|         0.68|                   21.62|               10.5|               122|    1|
|41100|Chungcheongnam-do|      Asan-si|36.789844| 127.00242|                     46|                60|               3|         1.29|                   12.89|                6.1|               435|    1|
|50020|     Jeollabuk-do|    Gunsan-si|35.967631|126.737011|                     56|                67|               5|         2.04|                   18.07|                8.8|               494|    1|
|60020| Gyeongsangbuk-do|  Gyeongju-si|35.856185|129.224796|                     43|                61|               4|         1.34|                   21.66|               11.0|               407|    1|
|12040|            Daegu|      Dong-gu|35.886836|128.635537|                     32|                58|               0|         1.15|                   18.81|                9.0|               649|    1|
|10040|            Seoul|   Gangseo-gu|37.551166|126.849506|                     36|                56|               1|         1.17|                   14.39|                5.7|              1080|    1|
+-----+-----------------+-------------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+-----+
only showing top 20 rows

no duplicate records are pesent in df1
+----------+----+-----------------+---------+--------+--------+-----+
|      date|time|         province|confirmed|released|deceased|count|
+----------+----+-----------------+---------+--------+--------+-----+
|2020-01-23|  16|     Jeollabuk-do|        0|       0|       0|    1|
|2020-01-31|  16|Chungcheongbuk-do|        0|       0|       0|    1|
|2020-02-01|  16|          Incheon|        1|       0|       0|    1|
|2020-03-02|   0|Chungcheongnam-do|       78|       0|       0|    1|
|2020-03-15|   0|           Sejong|       39|       0|       0|    1|
|2020-03-29|   0|          Incheon|       58|      15|       0|    1|
|2020-04-07|   0| Gyeongsangbuk-do|     1317|     934|      46|    1|
|2020-05-10|   0| Gyeongsangnam-do|      117|     108|       0|    1|
|2020-05-12|   0|       Gangwon-do|       54|      41|       2|    1|
|2020-05-20|   0|          Gwangju|       30|      30|       0|    1|
|2020-05-21|   0|            Seoul|      756|     596|       4|    1|
|2020-05-30|   0|            Busan|      146|     138|       3|    1|
|2020-05-30|   0|          Gwangju|       32|      30|       0|    1|
|2020-06-10|   0|     Jeollanam-do|       20|      18|       0|    1|
|2020-06-13|   0|          Incheon|      302|     141|       0|    1|
|2020-01-23|  16|Chungcheongnam-do|        0|       0|       0|    1|
|2020-01-26|  16|      Gyeonggi-do|        2|       0|       0|    1|
|2020-02-14|  16|          Incheon|        1|       1|       0|    1|
|2020-03-11|   0|            Daegu|     5794|     102|      44|    1|
|2020-03-14|   0|          Gwangju|       15|       4|       0|    1|
+----------+----+-----------------+---------+--------+--------+-----+
only showing top 20 rows

no duplicate records are pesent in df1

#Use limit function for showcasing a limited number of records.
df1.limit(5).show()
df2.limit(5).show()
df3.limit(5).show()
     
+--------+--------+------------+-----+--------------------+---------+---------+----------+
| case_id|province|        city|group|      infection_case|confirmed| latitude| longitude|
+--------+--------+------------+-----+--------------------+---------+---------+----------+
| 1000001|   Seoul|  Yongsan-gu| true|       Itaewon Clubs|      139|37.538621|126.992652|
| 1000002|   Seoul|   Gwanak-gu| true|             Richway|      119| 37.48208|126.901384|
| 1000003|   Seoul|     Guro-gu| true| Guro-gu Call Center|       95|37.508163|126.884387|
| 1000004|   Seoul|Yangcheon-gu| true|Yangcheon Table T...|       43|37.546061|126.874209|
| 1000005|   Seoul|   Dobong-gu| true|     Day Care Center|       43|37.679422|127.044374|
+--------+--------+------------+-----+--------------------+---------+---------+----------+

+-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
| code|province|       city| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
+-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|10000|   Seoul|      Seoul|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
|10010|   Seoul| Gangnam-gu|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|10020|   Seoul|Gangdong-gu|37.530492|127.123837|                     27|                32|               0|         1.54|                   14.55|                5.4|              1023|
|10030|   Seoul| Gangbuk-gu|37.639938|127.025508|                     14|                21|               0|         0.67|                   19.49|                8.5|               628|
|10040|   Seoul| Gangseo-gu|37.551166|126.849506|                     36|                56|               1|         1.17|                   14.39|                5.7|              1080|
+-----+--------+-----------+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+

+----------+----+--------+---------+--------+--------+
|      date|time|province|confirmed|released|deceased|
+----------+----+--------+---------+--------+--------+
|2020-01-20|  16|   Seoul|        0|       0|       0|
|2020-01-20|  16|   Busan|        0|       0|       0|
|2020-01-20|  16|   Daegu|        0|       0|       0|
|2020-01-20|  16| Incheon|        1|       0|       0|
|2020-01-20|  16| Gwangju|        0|       0|       0|
+----------+----+--------+---------+--------+--------+


#If you find the column name is not suitable, change the column name.[optional]
df1.withColumnRenamed('confirmed','confirmed_case').show()
     
+--------+--------+---------------+-----+--------------------+--------------+---------+----------+
| case_id|province|           city|group|      infection_case|confirmed_case| latitude| longitude|
+--------+--------+---------------+-----+--------------------+--------------+---------+----------+
| 1000001|   Seoul|     Yongsan-gu| true|       Itaewon Clubs|           139|37.538621|126.992652|
| 1000002|   Seoul|      Gwanak-gu| true|             Richway|           119| 37.48208|126.901384|
| 1000003|   Seoul|        Guro-gu| true| Guro-gu Call Center|            95|37.508163|126.884387|
| 1000004|   Seoul|   Yangcheon-gu| true|Yangcheon Table T...|            43|37.546061|126.874209|
| 1000005|   Seoul|      Dobong-gu| true|     Day Care Center|            43|37.679422|127.044374|
| 1000006|   Seoul|        Guro-gu| true|Manmin Central Ch...|            41|37.481059|126.894343|
| 1000007|   Seoul|from other city| true|SMR Newly Planted...|            36|        -|         -|
| 1000008|   Seoul|  Dongdaemun-gu| true|       Dongan Church|            17|37.592888|127.056766|
| 1000009|   Seoul|from other city| true|Coupang Logistics...|            25|        -|         -|
| 1000010|   Seoul|      Gwanak-gu| true|     Wangsung Church|            30|37.481735|126.930121|
| 1000011|   Seoul|   Eunpyeong-gu| true|Eunpyeong St. Mar...|            14| 37.63369|  126.9165|
| 1000012|   Seoul|   Seongdong-gu| true|    Seongdong-gu APT|            13| 37.55713|  127.0403|
| 1000013|   Seoul|      Jongno-gu| true|Jongno Community ...|            10| 37.57681|   127.006|
| 1000014|   Seoul|     Gangnam-gu| true|Samsung Medical C...|             7| 37.48825| 127.08559|
| 1000015|   Seoul|        Jung-gu| true|Jung-gu Fashion C...|             7|37.562405|126.984377|
| 1000016|   Seoul|   Seodaemun-gu| true|  Yeonana News Class|             5|37.558147|126.943799|
| 1000017|   Seoul|      Jongno-gu| true|Korea Campus Crus...|             7|37.594782|126.968022|
| 1000018|   Seoul|     Gangnam-gu| true|Gangnam Yeoksam-d...|             6|        -|         -|
| 1000019|   Seoul|from other city| true|Daejeon door-to-d...|             1|        -|         -|
| 1000020|   Seoul|   Geumcheon-gu| true|Geumcheon-gu rice...|             6|        -|         -|
+--------+--------+---------------+-----+--------------------+--------------+---------+----------+
only showing top 20 rows


#Select the subset of the columns.
df1.select('city','confirmed').show(30)
     
+---------------+---------+
|           city|confirmed|
+---------------+---------+
|     Yongsan-gu|      139|
|      Gwanak-gu|      119|
|        Guro-gu|       95|
|   Yangcheon-gu|       43|
|      Dobong-gu|       43|
|        Guro-gu|       41|
|from other city|       36|
|  Dongdaemun-gu|       17|
|from other city|       25|
|      Gwanak-gu|       30|
|   Eunpyeong-gu|       14|
|   Seongdong-gu|       13|
|      Jongno-gu|       10|
|     Gangnam-gu|        7|
|        Jung-gu|        7|
|   Seodaemun-gu|        5|
|      Jongno-gu|        7|
|     Gangnam-gu|        6|
|from other city|        1|
|   Geumcheon-gu|        6|
|from other city|        8|
|from other city|        5|
|        Jung-gu|       13|
|Yeongdeungpo-gu|        3|
|     Gangnam-gu|        1|
|   Yangcheon-gu|        3|
|      Seocho-gu|        5|
|from other city|        1|
|     Gangnam-gu|        4|
|     Gangseo-gu|        0|
+---------------+---------+
only showing top 30 rows


#df1.dropna(how='any',subset=["city","latitude","longitude"]).show()
#df1.show(50)
#df3.dropna(subset=['confirmed']).show()
df1=df1.withColumn("latitude",when(df1["latitude"]=="-",None).otherwise(df1["latitude"]))
df1=df1.withColumn("city",when(df1["city"]=="-",None).otherwise(df1["city"]))
df1.dropna().show(10)
#df2.dropna().show(50)
#df3.dropna().show(50)
     
+--------+--------+-------------+-----+--------------------+---------+---------+----------+
| case_id|province|         city|group|      infection_case|confirmed| latitude| longitude|
+--------+--------+-------------+-----+--------------------+---------+---------+----------+
| 1000001|   Seoul|   Yongsan-gu| true|       Itaewon Clubs|      139|37.538621|126.992652|
| 1000002|   Seoul|    Gwanak-gu| true|             Richway|      119| 37.48208|126.901384|
| 1000003|   Seoul|      Guro-gu| true| Guro-gu Call Center|       95|37.508163|126.884387|
| 1000004|   Seoul| Yangcheon-gu| true|Yangcheon Table T...|       43|37.546061|126.874209|
| 1000005|   Seoul|    Dobong-gu| true|     Day Care Center|       43|37.679422|127.044374|
| 1000006|   Seoul|      Guro-gu| true|Manmin Central Ch...|       41|37.481059|126.894343|
| 1000008|   Seoul|Dongdaemun-gu| true|       Dongan Church|       17|37.592888|127.056766|
| 1000010|   Seoul|    Gwanak-gu| true|     Wangsung Church|       30|37.481735|126.930121|
| 1000011|   Seoul| Eunpyeong-gu| true|Eunpyeong St. Mar...|       14| 37.63369|  126.9165|
| 1000012|   Seoul| Seongdong-gu| true|    Seongdong-gu APT|       13| 37.55713|  127.0403|
+--------+--------+-------------+-----+--------------------+---------+---------+----------+
only showing top 10 rows


'''
Filter the data based on different columns or variables and do the best analysis.
For example: We can filter a data frame using multiple
conditions using AND(&), OR(|) and NOT(~) conditions. For
example, we may want to find out all the different
infection_case in Daegu Province with more than 10
confirmed cases.
'''
#1
sel=df1.filter((col('province')=='Daegu') & (col('confirmed')>10))
ans=sel.select('infection_case').show(50)
#2Total Confirmed Cases by Province:
tcbbp=df1.groupBy('province').sum('confirmed').show()
#3Total Confirmed Cases for Each Infection Case:
tcci=df1.groupby('infection_case').sum('confirmed').show()
#4 Average Confirmed Cases by Group:
ac=df1.groupby('group').avg('confirmed').show()
#5 Top Cities with Most Confirmed Cases:
tc=df1.orderBy(col('confirmed').desc()).select('city','confirmed').show()
#6 Province-wise Spread of Different Infection Cases:
tcc=df1.groupby('province','infection_case').agg(count("*")).show()
#7 Cities with Confirmed Cases Over 15:
c=df1.filter(col('confirmed')>15).select('city','confirmed').show()
#8 Cities with Confirmed Cases Over 15:
cc=df1.orderBy(col('confirmed').desc()).select('infection_case','confirmed').show()
     
+--------------------+
|      infection_case|
+--------------------+
|  Shincheonji Church|
|Second Mi-Ju Hosp...|
|Hansarang Convale...|
|Daesil Convalesce...|
|     Fatima Hospital|
|     overseas inflow|
|contact with patient|
|                 etc|
+--------------------+

+-----------------+--------------+
|         province|sum(confirmed)|
+-----------------+--------------+
|           Sejong|            49|
|            Ulsan|            51|
|Chungcheongbuk-do|            60|
|       Gangwon-do|            62|
|          Gwangju|            43|
| Gyeongsangbuk-do|          1324|
|            Daegu|          6680|
| Gyeongsangnam-do|           132|
|          Incheon|           202|
|          Jeju-do|            19|
|      Gyeonggi-do|          1000|
|            Busan|           156|
|          Daejeon|           131|
|            Seoul|          1280|
|Chungcheongnam-do|           158|
|     Jeollabuk-do|            23|
|     Jeollanam-do|            25|
+-----------------+--------------+

+--------------------+--------------+
|      infection_case|sum(confirmed)|
+--------------------+--------------+
|     Dreaming Church|             4|
|Uiwang Logistics ...|             2|
|  Gwangneuksa Temple|             5|
|Eunpyeong St. Mar...|            14|
|Ministry of Ocean...|            31|
|     Day Care Center|            43|
|Korea Campus Crus...|            14|
|Yeongdeungpo Lear...|             3|
|Bundang Jesaeng H...|            22|
|       Milal Shelter|            36|
|     Wangsung Church|            36|
|       Itaewon Clubs|           271|
| Wonju-si Apartments|             4|
|       Dongan Church|            17|
|Samsung Medical C...|             7|
|   KB Life Insurance|            13|
|     overseas inflow|           949|
|Gyeongsan Cham Jo...|            16|
|Hansarang Convale...|           124|
|Seoul City Hall S...|             8|
+--------------------+--------------+
only showing top 20 rows

+-----+------------------+
|group|    avg(confirmed)|
+-----+------------------+
| true|63.314516129032256|
|false|             70.88|
+-----+------------------+

+---------------+---------+
|           city|confirmed|
+---------------+---------+
|         Nam-gu|     4511|
|              -|      917|
|              -|      747|
|from other city|      566|
|              -|      305|
|              -|      298|
|   Dalseong-gun|      196|
|              -|      190|
|              -|      162|
|     Yongsan-gu|      139|
|              -|      133|
|         Seo-gu|      124|
|      Gwanak-gu|      119|
|   Cheongdo-gun|      119|
|     Cheonan-si|      103|
|   Dalseong-gun|      101|
|              -|      100|
|        Guro-gu|       95|
|              -|       84|
|              -|       68|
+---------------+---------+
only showing top 20 rows

+-----------------+--------------------+--------+
|         province|      infection_case|count(1)|
+-----------------+--------------------+--------+
|     Jeollanam-do|     overseas inflow|       1|
|            Seoul|Jongno Community ...|       1|
|       Gangwon-do|  Shincheonji Church|       1|
|     Jeollabuk-do|  Shincheonji Church|       1|
|           Sejong|     overseas inflow|       1|
| Gyeongsangbuk-do|Gyeongsan Seorin ...|       1|
|Chungcheongnam-do|gym facility in C...|       1|
|            Seoul|     Daezayeon Korea|       1|
|            Busan|                 etc|       1|
|            Seoul|Eunpyeong St. Mar...|       1|
|          Gwangju|                 etc|       1|
|      Gyeonggi-do|Seongnam neighbor...|       1|
| Gyeongsangbuk-do|Gyeongsan Cham Jo...|       1|
|      Gyeonggi-do| Guro-gu Call Center|       1|
|            Ulsan|     overseas inflow|       1|
|      Gyeonggi-do|Uijeongbu St. Mar...|       1|
|     Jeollanam-do|Manmin Central Ch...|       1|
|       Gangwon-do|Uijeongbu St. Mar...|       1|
|          Daejeon|         Orange Town|       1|
|           Sejong|                 etc|       1|
+-----------------+--------------------+--------+
only showing top 20 rows

+---------------+---------+
|           city|confirmed|
+---------------+---------+
|     Yongsan-gu|      139|
|      Gwanak-gu|      119|
|        Guro-gu|       95|
|   Yangcheon-gu|       43|
|      Dobong-gu|       43|
|        Guro-gu|       41|
|from other city|       36|
|  Dongdaemun-gu|       17|
|from other city|       25|
|      Gwanak-gu|       30|
|              -|      298|
|              -|      162|
|              -|      100|
|     Dongnae-gu|       39|
|              -|       36|
|              -|       19|
|              -|       30|
|         Nam-gu|     4511|
|   Dalseong-gun|      196|
|         Seo-gu|      124|
+---------------+---------+
only showing top 20 rows

+--------------------+---------+
|      infection_case|confirmed|
+--------------------+---------+
|  Shincheonji Church|     4511|
|contact with patient|      917|
|                 etc|      747|
|  Shincheonji Church|      566|
|     overseas inflow|      305|
|     overseas inflow|      298|
|Second Mi-Ju Hosp...|      196|
|contact with patient|      190|
|contact with patient|      162|
|       Itaewon Clubs|      139|
|                 etc|      133|
|Hansarang Convale...|      124|
|             Richway|      119|
|Cheongdo Daenam H...|      119|
|gym facility in C...|      103|
|Daesil Convalesce...|      101|
|                 etc|      100|
| Guro-gu Call Center|       95|
|                 etc|       84|
|     overseas inflow|       68|
+--------------------+---------+
only showing top 20 rows


#Sort the number of confirmed cases. Confirmed column is there in the dataset. Check with descending sort also
df1.orderBy(col('confirmed').desc()).select('infection_case','confirmed').show()
df1.orderBy(col('confirmed').asc()).select('infection_case','confirmed').show()
     
+--------------------+---------+
|      infection_case|confirmed|
+--------------------+---------+
|  Shincheonji Church|     4511|
|contact with patient|      917|
|                 etc|      747|
|  Shincheonji Church|      566|
|     overseas inflow|      305|
|     overseas inflow|      298|
|Second Mi-Ju Hosp...|      196|
|contact with patient|      190|
|contact with patient|      162|
|       Itaewon Clubs|      139|
|                 etc|      133|
|Hansarang Convale...|      124|
|             Richway|      119|
|Cheongdo Daenam H...|      119|
|gym facility in C...|      103|
|Daesil Convalesce...|      101|
|                 etc|      100|
| Guro-gu Call Center|       95|
|                 etc|       84|
|     overseas inflow|       68|
+--------------------+---------+
only showing top 20 rows

+--------------------+---------+
|      infection_case|confirmed|
+--------------------+---------+
|SJ Investment Cal...|        0|
|contact with patient|        0|
|contact with patient|        0|
|Cheongdo Daenam H...|        1|
|Gangnam Dongin Ch...|        1|
|Anyang Gunpo Past...|        1|
|Daejeon door-to-d...|        1|
|         Orange Life|        1|
|                 etc|        1|
|                 etc|        1|
|  Shincheonji Church|        1|
|  Shincheonji Church|        1|
|  Shincheonji Church|        1|
|       Itaewon Clubs|        1|
|       Itaewon Clubs|        2|
|Cheongdo Daenam H...|        2|
|  Shincheonji Church|        2|
|  Shincheonji Church|        2|
|Seosan-si Laboratory|        2|
|Uiwang Logistics ...|        2|
+--------------------+---------+
only showing top 20 rows


#In case of any wrong data type, cast that data type from integer to string or string to integer.
df1=df1.withColumn("confirmed",df1["confirmed"].cast("int"))
df1=df1.withColumn("latitude",df1["latitude"].cast("float"))
df1=df1.withColumn("longitude",df1["longitude"].cast("float"))
df1.printSchema()
df1.show(3)
     
root
 |--  case_id: integer (nullable = true)
 |-- province: string (nullable = true)
 |-- city: string (nullable = true)
 |-- group: boolean (nullable = true)
 |-- infection_case: string (nullable = true)
 |-- confirmed: integer (nullable = true)
 |-- latitude: float (nullable = true)
 |-- longitude: float (nullable = true)

+--------+--------+----------+-----+-------------------+---------+---------+---------+
| case_id|province|      city|group|     infection_case|confirmed| latitude|longitude|
+--------+--------+----------+-----+-------------------+---------+---------+---------+
| 1000001|   Seoul|Yongsan-gu| true|      Itaewon Clubs|      139| 37.53862|    126.0|
| 1000002|   Seoul| Gwanak-gu| true|            Richway|      119| 37.48208|    126.0|
| 1000003|   Seoul|   Guro-gu| true|Guro-gu Call Center|       95|37.508163|    126.0|
+--------+--------+----------+-----+-------------------+---------+---------+---------+
only showing top 3 rows


#Use group by on top of province and city column and agg it with sum of confirmed cases
df1.groupby('province','city').agg(sum('confirmed').alias('confirmed')).show()
     
+----------------+---------------+---------+
|        province|           city|confirmed|
+----------------+---------------+---------+
|Gyeongsangnam-do|       Jinju-si|        9|
|           Seoul|        Guro-gu|      139|
|           Seoul|     Gangnam-gu|       18|
|         Daejeon|              -|      100|
|    Jeollabuk-do|from other city|        6|
|Gyeongsangnam-do|Changnyeong-gun|        7|
|           Seoul|              -|      561|
|         Jeju-do|from other city|        1|
|Gyeongsangbuk-do|              -|      345|
|Gyeongsangnam-do|   Geochang-gun|       18|
|Gyeongsangbuk-do|        Gumi-si|       10|
|         Incheon|from other city|      117|
|           Busan|              -|       85|
|           Daegu|         Seo-gu|      124|
|           Busan|     Suyeong-gu|        5|
|     Gyeonggi-do|   Uijeongbu-si|       50|
|           Seoul|     Yongsan-gu|      139|
|           Daegu|              -|     1705|
|           Seoul|   Seodaemun-gu|        5|
|     Gyeonggi-do|    Seongnam-si|       94|
+----------------+---------------+---------+
only showing top 20 rows


#For joins we will need one more file.you can use region file.User different different join methods
inn=df1.join(df2,on=['province','city'],how='inner').show()
inn=df1.join(df2,on=['province','city'],how='left').show()
inn=df1.join(df2,on=['province','city'],how='right').show()
inn=df1.join(df2,on=['province','city'],how='outer').show()
     
+--------+---------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|province|           city| case_id|group|      infection_case|confirmed| latitude|longitude| code| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
+--------+---------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|   Seoul|     Gangnam-gu| 1000029| true|Samsung Fire & Ma...|        4| 37.49828|    127.0|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|     Gangnam-gu| 1000025| true|Gangnam Dongin Ch...|        1| 37.52233|    127.0|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|     Gangnam-gu| 1000018| true|Gangnam Yeoksam-d...|        6|     null|     null|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|     Gangnam-gu| 1000014| true|Samsung Medical C...|        7| 37.48825|    127.0|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|     Gangseo-gu| 1000030| true|SJ Investment Cal...|        0| 37.55965|    126.0|10040|37.551166|126.849506|                     36|                56|               1|         1.17|                   14.39|                5.7|              1080|
|   Seoul|      Gwanak-gu| 1000010| true|     Wangsung Church|       30|37.481735|    126.0|10050| 37.47829|126.951502|                     22|                33|               1|         0.89|                   15.12|                4.9|               909|
|   Seoul|      Gwanak-gu| 1000002| true|             Richway|      119| 37.48208|    126.0|10050| 37.47829|126.951502|                     22|                33|               1|         0.89|                   15.12|                4.9|               909|
|   Seoul|        Guro-gu| 1000035| true|     Daezayeon Korea|        3|37.486835|    126.0|10070|37.495632| 126.88765|                     26|                34|               3|          1.0|                   16.21|                5.7|               741|
|   Seoul|        Guro-gu| 1000006| true|Manmin Central Ch...|       41| 37.48106|    126.0|10070|37.495632| 126.88765|                     26|                34|               3|          1.0|                   16.21|                5.7|               741|
|   Seoul|        Guro-gu| 1000003| true| Guro-gu Call Center|       95|37.508163|    126.0|10070|37.495632| 126.88765|                     26|                34|               3|          1.0|                   16.21|                5.7|               741|
|   Seoul|   Geumcheon-gu| 1000020| true|Geumcheon-gu rice...|        6|     null|     null|10080|37.456852|126.895229|                     18|                19|               0|         0.96|                   16.15|                6.7|               475|
|   Seoul|      Dobong-gu| 1000005| true|     Day Care Center|       43| 37.67942|    127.0|10100|37.668952|127.047082|                     23|                26|               1|         0.95|                   17.89|                7.2|               485|
|   Seoul|  Dongdaemun-gu| 1000008| true|       Dongan Church|       17|37.592888|    127.0|10110|37.574552|127.039721|                     21|                31|               4|         1.06|                   17.26|                6.7|               832|
|   Seoul|   Seodaemun-gu| 1000016| true|  Yeonana News Class|        5|37.558147|    126.0|10140|37.579428|126.936771|                     19|                25|               6|         1.12|                   16.77|                6.2|               587|
|   Seoul|      Seocho-gu| 1000027| true|       Seocho Family|        5|     null|     null|10150|37.483804|127.032693|                     24|                27|               1|          2.6|                   13.39|                3.8|              1465|
|   Seoul|   Seongdong-gu| 1000012| true|    Seongdong-gu APT|       13| 37.55713|    127.0|10160|37.563277|127.036647|                     21|                30|               2|         0.97|                   14.76|                5.3|               593|
|   Seoul|   Yangcheon-gu| 1000026| true|Biblical Language...|        3|37.524624|    126.0|10190|37.517189|126.866618|                     30|                43|               0|         2.26|                   13.55|                5.5|               816|
|   Seoul|   Yangcheon-gu| 1000004| true|Yangcheon Table T...|       43|37.546062|    126.0|10190|37.517189|126.866618|                     30|                43|               0|         2.26|                   13.55|                5.5|               816|
|   Seoul|Yeongdeungpo-gu| 1000024| true|Yeongdeungpo Lear...|        3|37.520847|    126.0|10200|37.526505| 126.89619|                     23|                39|               0|         1.21|                    15.6|                5.8|              1001|
|   Seoul|     Yongsan-gu| 1000001| true|       Itaewon Clubs|      139| 37.53862|    126.0|10210|37.532768|126.990021|                     15|                13|               1|         0.68|                   16.87|                6.5|               435|
+--------+---------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
only showing top 20 rows

+--------+---------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|province|           city| case_id|group|      infection_case|confirmed| latitude|longitude| code| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
+--------+---------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|   Seoul|     Yongsan-gu| 1000001| true|       Itaewon Clubs|      139| 37.53862|    126.0|10210|37.532768|126.990021|                     15|                13|               1|         0.68|                   16.87|                6.5|               435|
|   Seoul|      Gwanak-gu| 1000002| true|             Richway|      119| 37.48208|    126.0|10050| 37.47829|126.951502|                     22|                33|               1|         0.89|                   15.12|                4.9|               909|
|   Seoul|        Guro-gu| 1000003| true| Guro-gu Call Center|       95|37.508163|    126.0|10070|37.495632| 126.88765|                     26|                34|               3|          1.0|                   16.21|                5.7|               741|
|   Seoul|   Yangcheon-gu| 1000004| true|Yangcheon Table T...|       43|37.546062|    126.0|10190|37.517189|126.866618|                     30|                43|               0|         2.26|                   13.55|                5.5|               816|
|   Seoul|      Dobong-gu| 1000005| true|     Day Care Center|       43| 37.67942|    127.0|10100|37.668952|127.047082|                     23|                26|               1|         0.95|                   17.89|                7.2|               485|
|   Seoul|        Guro-gu| 1000006| true|Manmin Central Ch...|       41| 37.48106|    126.0|10070|37.495632| 126.88765|                     26|                34|               3|          1.0|                   16.21|                5.7|               741|
|   Seoul|from other city| 1000007| true|SMR Newly Planted...|       36|     null|     null| null|     null|      null|                   null|              null|            null|         null|                    null|               null|              null|
|   Seoul|  Dongdaemun-gu| 1000008| true|       Dongan Church|       17|37.592888|    127.0|10110|37.574552|127.039721|                     21|                31|               4|         1.06|                   17.26|                6.7|               832|
|   Seoul|from other city| 1000009| true|Coupang Logistics...|       25|     null|     null| null|     null|      null|                   null|              null|            null|         null|                    null|               null|              null|
|   Seoul|      Gwanak-gu| 1000010| true|     Wangsung Church|       30|37.481735|    126.0|10050| 37.47829|126.951502|                     22|                33|               1|         0.89|                   15.12|                4.9|               909|
|   Seoul|   Eunpyeong-gu| 1000011| true|Eunpyeong St. Mar...|       14| 37.63369|    126.0|10220|37.603481|126.929173|                     31|                44|               1|         1.09|                    17.0|                6.5|               874|
|   Seoul|   Seongdong-gu| 1000012| true|    Seongdong-gu APT|       13| 37.55713|    127.0|10160|37.563277|127.036647|                     21|                30|               2|         0.97|                   14.76|                5.3|               593|
|   Seoul|      Jongno-gu| 1000013| true|Jongno Community ...|       10| 37.57681|    127.0|10230|37.572999|126.979189|                     13|                17|               3|         1.71|                   18.27|                6.8|               668|
|   Seoul|     Gangnam-gu| 1000014| true|Samsung Medical C...|        7| 37.48825|    127.0|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|        Jung-gu| 1000015| true|Jung-gu Fashion C...|        7|37.562405|    126.0|10240|37.563988| 126.99753|                     12|                14|               2|         0.94|                   18.42|                7.4|               728|
|   Seoul|   Seodaemun-gu| 1000016| true|  Yeonana News Class|        5|37.558147|    126.0|10140|37.579428|126.936771|                     19|                25|               6|         1.12|                   16.77|                6.2|               587|
|   Seoul|      Jongno-gu| 1000017| true|Korea Campus Crus...|        7|37.594784|    126.0|10230|37.572999|126.979189|                     13|                17|               3|         1.71|                   18.27|                6.8|               668|
|   Seoul|     Gangnam-gu| 1000018| true|Gangnam Yeoksam-d...|        6|     null|     null|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|from other city| 1000019| true|Daejeon door-to-d...|        1|     null|     null| null|     null|      null|                   null|              null|            null|         null|                    null|               null|              null|
|   Seoul|   Geumcheon-gu| 1000020| true|Geumcheon-gu rice...|        6|     null|     null|10080|37.456852|126.895229|                     18|                19|               0|         0.96|                   16.15|                6.7|               475|
+--------+---------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
only showing top 20 rows

+--------+-------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|province|         city| case_id|group|      infection_case|confirmed| latitude|longitude| code| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
+--------+-------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|   Seoul|        Seoul|    null| null|                null|     null|     null|     null|10000|37.566953|126.977977|                    607|               830|              48|         1.44|                   15.38|                5.8|             22739|
|   Seoul|   Gangnam-gu| 1000029| true|Samsung Fire & Ma...|        4| 37.49828|    127.0|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|   Gangnam-gu| 1000025| true|Gangnam Dongin Ch...|        1| 37.52233|    127.0|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|   Gangnam-gu| 1000018| true|Gangnam Yeoksam-d...|        6|     null|     null|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|   Gangnam-gu| 1000014| true|Samsung Medical C...|        7| 37.48825|    127.0|10010|37.518421|127.047222|                     33|                38|               0|         4.18|                   13.17|                4.3|              3088|
|   Seoul|  Gangdong-gu|    null| null|                null|     null|     null|     null|10020|37.530492|127.123837|                     27|                32|               0|         1.54|                   14.55|                5.4|              1023|
|   Seoul|   Gangbuk-gu|    null| null|                null|     null|     null|     null|10030|37.639938|127.025508|                     14|                21|               0|         0.67|                   19.49|                8.5|               628|
|   Seoul|   Gangseo-gu| 1000030| true|SJ Investment Cal...|        0| 37.55965|    126.0|10040|37.551166|126.849506|                     36|                56|               1|         1.17|                   14.39|                5.7|              1080|
|   Seoul|    Gwanak-gu| 1000010| true|     Wangsung Church|       30|37.481735|    126.0|10050| 37.47829|126.951502|                     22|                33|               1|         0.89|                   15.12|                4.9|               909|
|   Seoul|    Gwanak-gu| 1000002| true|             Richway|      119| 37.48208|    126.0|10050| 37.47829|126.951502|                     22|                33|               1|         0.89|                   15.12|                4.9|               909|
|   Seoul|  Gwangjin-gu|    null| null|                null|     null|     null|     null|10060|37.538712|127.082366|                     22|                33|               3|         1.16|                   13.75|                4.8|               723|
|   Seoul|      Guro-gu| 1000035| true|     Daezayeon Korea|        3|37.486835|    126.0|10070|37.495632| 126.88765|                     26|                34|               3|          1.0|                   16.21|                5.7|               741|
|   Seoul|      Guro-gu| 1000006| true|Manmin Central Ch...|       41| 37.48106|    126.0|10070|37.495632| 126.88765|                     26|                34|               3|          1.0|                   16.21|                5.7|               741|
|   Seoul|      Guro-gu| 1000003| true| Guro-gu Call Center|       95|37.508163|    126.0|10070|37.495632| 126.88765|                     26|                34|               3|          1.0|                   16.21|                5.7|               741|
|   Seoul| Geumcheon-gu| 1000020| true|Geumcheon-gu rice...|        6|     null|     null|10080|37.456852|126.895229|                     18|                19|               0|         0.96|                   16.15|                6.7|               475|
|   Seoul|     Nowon-gu|    null| null|                null|     null|     null|     null|10090|37.654259|127.056294|                     42|                66|               6|         1.39|                    15.4|                7.4|               952|
|   Seoul|    Dobong-gu| 1000005| true|     Day Care Center|       43| 37.67942|    127.0|10100|37.668952|127.047082|                     23|                26|               1|         0.95|                   17.89|                7.2|               485|
|   Seoul|Dongdaemun-gu| 1000008| true|       Dongan Church|       17|37.592888|    127.0|10110|37.574552|127.039721|                     21|                31|               4|         1.06|                   17.26|                6.7|               832|
|   Seoul|   Dongjak-gu|    null| null|                null|     null|     null|     null|10120|37.510571|126.963604|                     21|                34|               3|         1.17|                   15.85|                5.2|               762|
|   Seoul|      Mapo-gu|    null| null|                null|     null|     null|     null|10130|37.566283|126.901644|                     22|                24|               2|         1.83|                   14.05|                4.9|               929|
+--------+-------------+--------+-----+--------------------+---------+---------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
only showing top 20 rows

+--------+------------+--------+-----+--------------------+---------+--------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|province|        city| case_id|group|      infection_case|confirmed|latitude|longitude| code| latitude| longitude|elementary_school_count|kindergarten_count|university_count|academy_ratio|elderly_population_ratio|elderly_alone_ratio|nursing_home_count|
+--------+------------+--------+-----+--------------------+---------+--------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
|   Busan|           -| 1100008|false|     overseas inflow|       36|    null|     null| null|     null|      null|                   null|              null|            null|         null|                    null|               null|              null|
|   Busan|           -| 1100009|false|contact with patient|       19|    null|     null| null|     null|      null|                   null|              null|            null|         null|                    null|               null|              null|
|   Busan|           -| 1100010|false|                 etc|       30|    null|     null| null|     null|      null|                   null|              null|            null|         null|                    null|               null|              null|
|   Busan|      Buk-gu|    null| null|                null|     null|    null|     null|11080|35.197483|128.990224|                     27|                37|               1|         1.35|                   16.04|                8.4|               465|
|   Busan|       Busan|    null| null|                null|     null|    null|     null|11000|35.179884|129.074796|                    304|               408|              22|          1.4|                   18.41|                8.6|              6752|
|   Busan| Busanjin-gu|    null| null|                null|     null|    null|     null|11070|35.163332|129.053058|                     32|                43|               3|         1.62|                   19.01|                8.7|               986|
|   Busan|     Dong-gu|    null| null|                null|     null|    null|     null|11050|35.129432| 129.04554|                      7|                 9|               0|         0.83|                   25.42|               13.8|               239|
|   Busan|  Dongnae-gu| 1100001| true|       Onchun Church|       39|35.21628|    129.0|11060| 35.20506|129.083673|                     22|                31|               0|         1.98|                   17.53|                7.7|               608|
|   Busan|  Gangseo-gu|    null| null|                null|     null|    null|     null|11010|35.212424| 128.98068|                     17|                21|               0|         1.43|                   11.84|                5.0|               147|
|   Busan|Geumjeong-gu|    null| null|                null|     null|    null|     null|11020|35.243053|129.092163|                     22|                28|               4|         1.64|                    19.8|                8.4|               466|
|   Busan|  Gijang-gun|    null| null|                null|     null|    null|     null|11030|35.244881|129.222253|                     21|                35|               0|         1.31|                   15.45|                7.4|               229|
|   Busan| Haeundae-gu| 1100004| true|Haeundae-gu Catho...|        6|35.20599|    129.0|11160| 35.16336|129.163594|                     33|                39|               1|         1.63|                   16.53|                7.9|               814|
|   Busan|      Jin-gu| 1100005| true|      Jin-gu Academy|        4|35.17371|    129.0| null|     null|      null|                   null|              null|            null|         null|                    null|               null|              null|
|   Busan|     Jung-gu|    null| null|                null|     null|    null|     null|11150|35.106321|129.032256|                      4|                 4|               0|         1.36|                    26.0|               13.0|               182|
|   Busan|      Nam-gu|    null| null|                null|     null|    null|     null|11040|35.136789| 129.08414|                     21|                27|               4|         1.24|                   19.13|                7.9|               475|
|   Busan|     Saha-gu|    null| null|                null|     null|    null|     null|11100|35.104642|128.974792|                     26|                42|               2|         1.34|                   17.76|                8.1|               541|
|   Busan|   Sasang-gu|    null| null|                null|     null|    null|     null|11090|35.152777|128.991142|                     21|                31|               3|         0.78|                    17.0|                8.1|               317|
|   Busan|      Seo-gu|    null| null|                null|     null|    null|     null|11110|35.098135|129.024193|                     11|                 9|               0|         1.02|                   24.36|               12.3|               235|
|   Busan|  Suyeong-gu| 1100003| true|Suyeong-gu Kinder...|        5|35.16708|    129.0|11120|35.145805|129.113194|                     10|                22|               0|         1.56|                    20.4|                8.2|               395|
|   Busan|  Yeongdo-gu|    null| null|                null|     null|    null|     null|11140|35.091362|129.067884|                     14|                12|               2|         0.69|                   26.13|               13.3|               203|
+--------+------------+--------+-----+--------------------+---------+--------+---------+-----+---------+----------+-----------------------+------------------+----------------+-------------+------------------------+-------------------+------------------+
only showing top 20 rows


#SQL
df1.createOrReplaceTempView('case')
spark.sql("select *from case").show()
spark.sql("select city,province,sum(confirmed) as total from case \
          group by city,province \
          order by total desc").show()
spark.sql("select city,province,confirmed from case\
           order by confirmed").show()
spark.sql("select city,province,sum(confirmed) as total from case \
           group by city,province \
           having total> 50 \
           order by total desc").show()
     
+--------+--------+---------------+-----+--------------------+---------+---------+---------+
| case_id|province|           city|group|      infection_case|confirmed| latitude|longitude|
+--------+--------+---------------+-----+--------------------+---------+---------+---------+
| 1000001|   Seoul|     Yongsan-gu| true|       Itaewon Clubs|      139| 37.53862|    126.0|
| 1000002|   Seoul|      Gwanak-gu| true|             Richway|      119| 37.48208|    126.0|
| 1000003|   Seoul|        Guro-gu| true| Guro-gu Call Center|       95|37.508163|    126.0|
| 1000004|   Seoul|   Yangcheon-gu| true|Yangcheon Table T...|       43|37.546062|    126.0|
| 1000005|   Seoul|      Dobong-gu| true|     Day Care Center|       43| 37.67942|    127.0|
| 1000006|   Seoul|        Guro-gu| true|Manmin Central Ch...|       41| 37.48106|    126.0|
| 1000007|   Seoul|from other city| true|SMR Newly Planted...|       36|     null|     null|
| 1000008|   Seoul|  Dongdaemun-gu| true|       Dongan Church|       17|37.592888|    127.0|
| 1000009|   Seoul|from other city| true|Coupang Logistics...|       25|     null|     null|
| 1000010|   Seoul|      Gwanak-gu| true|     Wangsung Church|       30|37.481735|    126.0|
| 1000011|   Seoul|   Eunpyeong-gu| true|Eunpyeong St. Mar...|       14| 37.63369|    126.0|
| 1000012|   Seoul|   Seongdong-gu| true|    Seongdong-gu APT|       13| 37.55713|    127.0|
| 1000013|   Seoul|      Jongno-gu| true|Jongno Community ...|       10| 37.57681|    127.0|
| 1000014|   Seoul|     Gangnam-gu| true|Samsung Medical C...|        7| 37.48825|    127.0|
| 1000015|   Seoul|        Jung-gu| true|Jung-gu Fashion C...|        7|37.562405|    126.0|
| 1000016|   Seoul|   Seodaemun-gu| true|  Yeonana News Class|        5|37.558147|    126.0|
| 1000017|   Seoul|      Jongno-gu| true|Korea Campus Crus...|        7|37.594784|    126.0|
| 1000018|   Seoul|     Gangnam-gu| true|Gangnam Yeoksam-d...|        6|     null|     null|
| 1000019|   Seoul|from other city| true|Daejeon door-to-d...|        1|     null|     null|
| 1000020|   Seoul|   Geumcheon-gu| true|Geumcheon-gu rice...|        6|     null|     null|
+--------+--------+---------------+-----+--------------------+---------+---------+---------+
only showing top 20 rows

+---------------+-----------------+-----+
|           city|         province|total|
+---------------+-----------------+-----+
|         Nam-gu|            Daegu| 4511|
|              -|            Daegu| 1705|
|from other city| Gyeongsangbuk-do|  607|
|              -|            Seoul|  561|
|              -|      Gyeonggi-do|  477|
|              -| Gyeongsangbuk-do|  345|
|   Dalseong-gun|            Daegu|  297|
|from other city|      Gyeonggi-do|  248|
|      Gwanak-gu|            Seoul|  149|
|        Guro-gu|            Seoul|  139|
|     Yongsan-gu|            Seoul|  139|
|         Seo-gu|            Daegu|  124|
|   Cheongdo-gun| Gyeongsangbuk-do|  119|
|from other city|          Incheon|  117|
|     Cheonan-si|Chungcheongnam-do|  103|
|              -|          Daejeon|  100|
|   Gyeongsan-si| Gyeongsangbuk-do|   99|
|    Seongnam-si|      Gyeonggi-do|   94|
|              -|          Incheon|   85|
|              -|            Busan|   85|
+---------------+-----------------+-----+
only showing top 20 rows

+---------------+------------+---------+
|           city|    province|confirmed|
+---------------+------------+---------+
|     Gangseo-gu|       Seoul|        0|
|              -|  Gangwon-do|        0|
|              -|     Jeju-do|        0|
|from other city|       Busan|        1|
|     Gangnam-gu|       Seoul|        1|
|from other city|       Seoul|        1|
|from other city|       Seoul|        1|
|              -|       Seoul|        1|
|              -|      Sejong|        1|
|              -|     Gwangju|        1|
|from other city|      Sejong|        1|
|from other city|Jeollabuk-do|        1|
|from other city|Jeollanam-do|        1|
|from other city|     Jeju-do|        1|
|from other city|       Daegu|        2|
|from other city|       Daegu|        2|
|from other city|     Incheon|        2|
|from other city|     Daejeon|        2|
|from other city|     Daejeon|        2|
|from other city|       Seoul|        2|
+---------------+------------+---------+
only showing top 20 rows

+---------------+-----------------+-----+
|           city|         province|total|
+---------------+-----------------+-----+
|         Nam-gu|            Daegu| 4511|
|              -|            Daegu| 1705|
|from other city| Gyeongsangbuk-do|  607|
|              -|            Seoul|  561|
|              -|      Gyeonggi-do|  477|
|              -| Gyeongsangbuk-do|  345|
|   Dalseong-gun|            Daegu|  297|
|from other city|      Gyeonggi-do|  248|
|      Gwanak-gu|            Seoul|  149|
|        Guro-gu|            Seoul|  139|
|     Yongsan-gu|            Seoul|  139|
|         Seo-gu|            Daegu|  124|
|   Cheongdo-gun| Gyeongsangbuk-do|  119|
|from other city|          Incheon|  117|
|     Cheonan-si|Chungcheongnam-do|  103|
|              -|          Daejeon|  100|
|   Gyeongsan-si| Gyeongsangbuk-do|   99|
|    Seongnam-si|      Gyeonggi-do|   94|
|              -|          Incheon|   85|
|              -|            Busan|   85|
+---------------+-----------------+-----+
only showing top 20 rows


#UDF
def casehighlow(confirmed):
  if confirmed>50:
    return "low"
  else:
    return "high"
categorize_cases_udf = spark.udf.register("categorize_cases_udf",casehighlow,StringType())
# Use when() function with the UDF
df_with_category = df1.withColumn("case_category", categorize_cases_udf(col("confirmed"))).show()

     
+--------+--------+---------------+-----+--------------------+---------+---------+---------+-------------+
| case_id|province|           city|group|      infection_case|confirmed| latitude|longitude|case_category|
+--------+--------+---------------+-----+--------------------+---------+---------+---------+-------------+
| 1000001|   Seoul|     Yongsan-gu| true|       Itaewon Clubs|      139| 37.53862|    126.0|          low|
| 1000002|   Seoul|      Gwanak-gu| true|             Richway|      119| 37.48208|    126.0|          low|
| 1000003|   Seoul|        Guro-gu| true| Guro-gu Call Center|       95|37.508163|    126.0|          low|
| 1000004|   Seoul|   Yangcheon-gu| true|Yangcheon Table T...|       43|37.546062|    126.0|         high|
| 1000005|   Seoul|      Dobong-gu| true|     Day Care Center|       43| 37.67942|    127.0|         high|
| 1000006|   Seoul|        Guro-gu| true|Manmin Central Ch...|       41| 37.48106|    126.0|         high|
| 1000007|   Seoul|from other city| true|SMR Newly Planted...|       36|     null|     null|         high|
| 1000008|   Seoul|  Dongdaemun-gu| true|       Dongan Church|       17|37.592888|    127.0|         high|
| 1000009|   Seoul|from other city| true|Coupang Logistics...|       25|     null|     null|         high|
| 1000010|   Seoul|      Gwanak-gu| true|     Wangsung Church|       30|37.481735|    126.0|         high|
| 1000011|   Seoul|   Eunpyeong-gu| true|Eunpyeong St. Mar...|       14| 37.63369|    126.0|         high|
| 1000012|   Seoul|   Seongdong-gu| true|    Seongdong-gu APT|       13| 37.55713|    127.0|         high|
| 1000013|   Seoul|      Jongno-gu| true|Jongno Community ...|       10| 37.57681|    127.0|         high|
| 1000014|   Seoul|     Gangnam-gu| true|Samsung Medical C...|        7| 37.48825|    127.0|         high|
| 1000015|   Seoul|        Jung-gu| true|Jung-gu Fashion C...|        7|37.562405|    126.0|         high|
| 1000016|   Seoul|   Seodaemun-gu| true|  Yeonana News Class|        5|37.558147|    126.0|         high|
| 1000017|   Seoul|      Jongno-gu| true|Korea Campus Crus...|        7|37.594784|    126.0|         high|
| 1000018|   Seoul|     Gangnam-gu| true|Gangnam Yeoksam-d...|        6|     null|     null|         high|
| 1000019|   Seoul|from other city| true|Daejeon door-to-d...|        1|     null|     null|         high|
| 1000020|   Seoul|   Geumcheon-gu| true|Geumcheon-gu rice...|        6|     null|     null|         high|
+--------+--------+---------------+-----+--------------------+---------+---------+---------+-------------+
only showing top 20 rows





