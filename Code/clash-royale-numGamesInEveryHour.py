from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
from time import time

t_0 = time()

spark = SparkSession.builder.appName("Number of games played in every hour") \
                            .config("spark.sql.session.timeZone", "UTC") \
                            .getOrCreate()
df = spark.read.csv(sys.argv[1], sep=',', inferSchema=True, header=True).coalesce(int(sys.argv[3]))

# Extramos la hora de la columna 'battleTime', agrupamos por la hora y contamos el numero de elementos con cada hora
# Ordenamos para tenerlo ordenado en funcion de la hora
# Usamos un filtro porque hay unos 100 elementos que no tenian hora
df_count = df.withColumn("hour_of_day", F.hour(F.col("battleTime"))) \
             .groupBy("hour_of_day").count().withColumnRenamed("count", "num_games") \
             .filter(F.col("hour_of_day").isNotNull()) \
             .orderBy(F.col("hour_of_day").asc())

df_count.write.option("header", "true").mode('overwrite').csv(sys.argv[2] + "/numGamesEveryHour")

print(time() - t_0, " seconds elapsed")