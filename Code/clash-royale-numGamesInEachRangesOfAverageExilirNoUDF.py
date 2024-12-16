from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
from time import time

t_0 = time()

spark = SparkSession.builder.appName("Number of games in each range of average elixir used").getOrCreate()
df = spark.read.csv(sys.argv[1], sep=',', inferSchema=True, header=True).coalesce(int(sys.argv[3]))

# Filtramos aquellas entradas que tiene el campo del elixir medio del ganador a nulo para evitar excepciones (son como 100 elementos)
# Extramos el elixir medio del ganador usado en cada partida, lo asociamos a un rango y contamos el numero de partidas en cada rango
# Ordenamos para tenerlo ordenado en funcion de la hora
# Usamos un filtro porque hay unos 100 elementos que no tenian hora
df_winner = df.filter(F.col("`winner.elixir.average`").isNotNull()) \
              .withColumn("elixir_range", F.when((col >= 1) & (col < 2), "1-2")
                                            .when((col >= 2) & (col < 3), "2-3")
                                            .when((col >= 3) & (col < 4), "3-4")
                                            .when((col >= 4) & (col < 5), "4-5")
                                            .when((col >= 5) & (col < 6), "5-6")
                                            .when((col >= 6) & (col < 7), "6-7")
                                            .when((col >= 7) & (col < 8), "7-8")
                                            .when((col >= 8) & (col < 9), "8-9")
                                            .otherwise("9-10")) \
              .groupBy("elixir_range").count().withColumnRenamed("count", "winner_num_games")

# Filtramos aquellas entradas que tiene el campo del elixir medio del ganador a nulo para evitar excepciones (son como 100 elementos)
# Extramos el elixir medio del perdedor usado en cada partida, lo asociamos a un rango y contamos el numero de partidas en cada rango
# Ordenamos para tenerlo ordenado en funcion de la hora
# Usamos un filtro porque hay unos 100 elementos que no tenian hora
df_loser = df.filter(F.col("`loser.elixir.average`").isNotNull()) \
             .withColumn("elixir_range", F.when((col >= 1) & (col < 2), "1-2")
                                            .when((col >= 2) & (col < 3), "2-3")
                                            .when((col >= 3) & (col < 4), "3-4")
                                            .when((col >= 4) & (col < 5), "4-5")
                                            .when((col >= 5) & (col < 6), "5-6")
                                            .when((col >= 6) & (col < 7), "6-7")
                                            .when((col >= 7) & (col < 8), "7-8")
                                            .when((col >= 8) & (col < 9), "8-9")
                                            .otherwise("9-10")) \
             .groupBy("elixir_range").count().withColumnRenamed("count", "loser_num_games")
             

# Hacemos un join y tenemos para cada rango cuantar partidas el jugador ganador esta en cada rango y lo mismo para el perdedor
df_final = df_winner.join(df_loser, on="elixir_range", how="left").orderBy(F.col("elixir_range").asc())

df_final.write.option("header", "true").mode('overwrite').csv(sys.argv[2] + "/numGamesInEachRangesOfAverageExilir")

print(time() - t_0, " seconds elapsed")