from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
from time import time

t_0 = time()

spark = SparkSession.builder.appName("Top 100 of player with most wins").getOrCreate()
df = spark.read.csv(sys.argv[1], sep=',', inferSchema=True, header=True).coalesce(int(sys.argv[3]))

# Cogemos el id del jugador que ha ganado de 'winner.tag', agrupamos por este y contamos el numero de elementos con cada tag
# Ordenamos para tenerlo ordenado en funcion del numero de victorias y nos quedamos con el top 100
df_count = df.select(F.col("`winner.tag`").alias("player_id")) \
             .groupBy("player_id").count().withColumnRenamed("count", "num_games") \
             .orderBy(F.col("num_games").desc()).limit(100)

df_count.write.option("header", "true").mode("overwrite").csv(sys.argv[2] + "/top100withMostWins")

print(time() - t_0, " seconds elapsed")