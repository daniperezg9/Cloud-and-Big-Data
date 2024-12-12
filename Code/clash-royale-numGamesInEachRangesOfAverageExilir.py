from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import sys
from time import time

# Nos creamos una funcion para asignar el rango en base al exilir medio de la partida 
def assignRange(elixir):
    if 1 <= elixir < 2:
        return "1-2"
    elif 2 <= elixir < 3:
        return "2-3"
    elif 3 <= elixir < 4:
        return "3-4"
    elif 4 <= elixir < 5:
        return "4-5"
    elif 5 <= elixir < 6:
        return "5-6"
    elif 6 <= elixir < 7:
        return "6-7"
    elif 7 <= elixir < 8:
        return "7-8"
    elif 8 <= elixir < 9:
        return "8-9"
    else: 
        return "9-10"

assignRangeUdf = F.udf(assignRange, StringType())

t_0 = time()

spark = SparkSession.builder.appName("Number of games in each range of average elixir used").getOrCreate()
df = spark.read.csv(sys.argv[1], sep=',', inferSchema=True, header=True).coalesce(int(sys.argv[3]))

# Filtramos aquellas entradas que tiene el campo del elixir medio del ganador a nulo para evitar excepciones (son como 100 elementos)
# Extramos el elixir medio del ganador usado en cada partida, lo asociamos a un rango y contamos el numero de partidas en cada rango
# Ordenamos para tenerlo ordenado en funcion de la hora
# Usamos un filtro porque hay unos 100 elementos que no tenian hora
df_winner = df.filter(F.col("`winner.elixir.average`").isNotNull()) \
              .withColumn("elixir_range", assignRangeUdf("`winner.elixir.average`")) \
              .groupBy("elixir_range").count().withColumnRenamed("count", "winner_num_games")

# Filtramos aquellas entradas que tiene el campo del elixir medio del ganador a nulo para evitar excepciones (son como 100 elementos)
# Extramos el elixir medio del perdedor usado en cada partida, lo asociamos a un rango y contamos el numero de partidas en cada rango
# Ordenamos para tenerlo ordenado en funcion de la hora
# Usamos un filtro porque hay unos 100 elementos que no tenian hora
df_loser = df.filter(F.col("`loser.elixir.average`").isNotNull()) \
             .withColumn("elixir_range", assignRangeUdf("`loser.elixir.average`")) \
             .groupBy("elixir_range").count().withColumnRenamed("count", "loser_num_games")
             

# Hacemos un join y tenemos para cada rango cuantar partidas el jugador ganador esta en cada rango y lo mismo para el perdedor
df_final = df_winner.join(df_loser, on="elixir_range", how="left").orderBy(F.col("elixir_range").asc())

df_final.write.option("header", "true").mode('overwrite').csv(sys.argv[2] + "/numGamesInEachRangesOfAverageExilir")

print(time() - t_0, " seconds elapsed")