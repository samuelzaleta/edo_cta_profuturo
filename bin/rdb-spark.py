from pyspark.sql import SparkSession

print('Conectando la sesión Spark...')
spark = SparkSession.builder.appName("RDB").getOrCreate()
print('Conectada la sesión Spark!')

print('Extrayendo...')
"""
df = spark.read.jdbc(
    'jdbc:rdbThin://130.40.30.144:1707/mexico$base:cierren@tracelevel=-1',
    '(SELECT CSIE1_NUMCUE FROM MOV_AVOL LIMIT 5) mov_avol',
    properties={"user": "obadillo", "password": "BADILLO2022"},
)
"""

df = spark.read.format('jdbc') \
    .option('driver', 'oracle.rdb.jdbc.rdbThin.Driver') \
    .option('url', 'jdbc:rdbThin://130.40.30.144:1707/mexico$base:cierren@tracelevel=-1') \
    .option('query', 'SELECT * FROM MOV_AVOL LIMIT 5') \
    .option('user', 'obadillo') \
    .option('password', 'BADILLO2022') \
    .load()

df.count()
