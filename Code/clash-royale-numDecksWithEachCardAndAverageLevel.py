from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
from time import time

t_0 = time()

##Definir las columnas que queremos coger (las que tienen los ids de las cartas y su nivel)
##Importante la comillas ``, si no pyspark no encuentra la columna
card_columns = [
    ("`winner.card1.id`", "`winner.card1.level`"), 
    ("`winner.card2.id`", "`winner.card2.level`"),
    ("`winner.card3.id`", "`winner.card3.level`"),
    ("`winner.card4.id`", "`winner.card4.level`"),
    ("`winner.card5.id`", "`winner.card5.level`"),
    ("`winner.card6.id`", "`winner.card6.level`"),
    ("`winner.card7.id`", "`winner.card7.level`"),
    ("`winner.card8.id`", "`winner.card8.level`"),
    ("`loser.card1.id`", "`loser.card1.level`"),
    ("`loser.card2.id`", "`loser.card2.level`"),
    ("`loser.card3.id`", "`loser.card3.level`"),
    ("`loser.card4.id`", "`loser.card4.level`"),
    ("`loser.card5.id`", "`loser.card5.level`"),
    ("`loser.card6.id`", "`loser.card6.level`"),
    ("`loser.card7.id`", "`loser.card7.level`"),
    ("`loser.card8.id`", "`loser.card8.level`")
]

spark = SparkSession.builder.appName("Number of decks which each card and its average level").getOrCreate()
df_games = spark.read.csv(sys.argv[1], sep=',', inferSchema=True, header=True).coalesce(int(sys.argv[4]))
df_names = spark.read.csv(sys.argv[2], sep=',', inferSchema=True, header=True)

# Creamos una columnas con un struct del id y nivel, a partir de las 16 columnas que nos dicen el mazo de la partida
df_cards = df_games.select(
    F.explode(F.array([F.struct(F.col(card_id).alias("card_id"), F.col(card_level).alias("card_level"))
                   for card_id, card_level in card_columns])).alias("card")
)

# Convertimos la columna del struct en 2 columnas
df_cards = df_cards.select("card.card_id", "card.card_level")

# Averiguamos el nivel medio de cada carta y el numero de veces que se ha usado en los mazos del dataset y unimos con el dataset que tiene los nombres
df_cards = df_cards.groupBy("card_id").agg(
        F.count("card_id").alias("num_decks_which_used_it"),
        F.avg("card_level").alias("avg_card_level")
    ).join(df_names, on="card_id", how="left")
                
df_cards = df_cards.select("card_name", "num_decks_which_used_it", "avg_card_level")

df_cards.write.option("header", "true").mode("overwrite").csv(sys.argv[3] + "/numTimesPlayedEachCardAndAverageLevel")

print(time() - t_0, " seconds elapsed")