from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import sys
from time import time

def checkMostPlayedType(troops, structures, spells):
    if troops > structures and troops > spells:
        return "troops"
    elif structures > troops and structures > spells:
        return "structures"
    elif spells > troops and spells > structures:
        return "spells"
    elif troops == structures and structures == spells:
        return "all the same"
    elif troops == structures:
        return "trps & strs"
    elif structures == spells:
        return "strs & spells"
    else:
        return "trps & spells"

checkMostPlayedTypeUdf = F.udf(checkMostPlayedType, StringType())

t_0 = time()

spark = SparkSession.builder.appName("Number of decks which each card and its average level").getOrCreate()
df = spark.read.csv(sys.argv[1], sep=',', inferSchema=True, header=True).coalesce(int(sys.argv[3]))

# Creamos una condicion que usaremos para filtrar los elementos que estan a nulo (aprox 100)
condition = (
    F.col("`winner.troop.count`").isNotNull() &
    F.col("`loser.troop.count`").isNotNull() &
    F.col("`winner.structure.count`").isNotNull() &
    F.col("`loser.structure.count`").isNotNull() &
    F.col("`winner.spell.count`").isNotNull() &
    F.col("`loser.spell.count`").isNotNull()
)

# Averiguamos el nivel medio de cada carta y el numero de veces que se ha usado en los mazos del dataset y unimos con el dataset que tiene los nombres
df_types = df.filter(condition) \
             .withColumn("total_troops_played", F.col("`winner.troop.count`") + F.col("`loser.troop.count`")) \
             .withColumn("total_structures_played", F.col("`winner.structure.count`") + F.col("`loser.structure.count`")) \
             .withColumn("total_spells_played", F.col("`winner.spell.count`") + F.col("`loser.spell.count`"))

df_types = df_types.select("total_troops_played", "total_structures_played", "total_spells_played")

df_count = df_types.withColumn("most_played_type", checkMostPlayedTypeUdf("total_troops_played", "total_structures_played", "total_spells_played")) \
                   .groupBy("most_played_type").count().withColumnRenamed("count", "num_games")

df_count.write.option("header", "true").mode("overwrite").csv(sys.argv[2] + "/checkMostPlayedTypeOfCardInEachGame")

print(time() - t_0, " seconds elapsed")