# Cloud & Big Data - Proyecto Final
## Descripcion del problema
El proyecto se centra en el análisis de partidas de la temporada 18 de Clash Royale, un popular juego de estrategia en tiempo real desarrollado por Supercell. Con información de unas aproximadamente 38 millones de partidas, ofrece una rica fuente de datos para estudiar diversos aspectos del comportamiento de los jugadores y las dinámicas del juego, como las cartas más jugadas o el uso de recursos (elixir).

Clash Royale enfrenta a dos jugadores en una arena, donde deben utilizar una combinación de cartas de tropas, hechizos y edificios para destruir las torres del oponente. Cada partida dura hasta 3 minutos, con tiempo extra si es necesario para determinar un ganador.

## Necesidad de procesamiento de Big Data y Cloud Computing
Clash Royale es un juego que cuenta con millones de usuarios activos jugando casi a diario, lo que hace necesario usar técnicas especializadas en el procesamiento masivo de datos. Cloud es una herramienta útil para el procesamiento de estos datos gracias a su capacidad de escalabilidad, flexibilidad y costos eficientes. Además, ofrece un rendimiento optimizado y seguridad robusta, que lo convierte en una solución ideal para manejar la inmensa cantidad de datos generado por los jugadores.

## Descripción de los datos
La información de las partidas se ha extraido de la siguiente página de datasets: [Partidas Clash Royale](https://www.kaggle.com/datasets/bwandowando/clash-royale-season-18-dec-0320-dataset). En ella se explica que los datos han sido extraidos mediante la API de Supercell para el juego. Tambien se explica que la información recolectada, aunque pueda parecer una gran cantidad (38 millones de partidas, de las cuales nosotros hemos cogido la informacion de unas 17 millones), no es nada comparado con la cantidad total de partidas jugadas durante la temporada 18.

Los archivos estan estructurados como csvs. Cada archivo presenta la información de distintas partidas jugadas durante la temporada 18. Para cada partida sabe el jugador vencedor y perdedor, el mazo que ha usado cada jugador, el numero de cartas de cada tipo (tropas, estructuras, hechizos) que ha usado cada jugador, asi como la calidad (común, rara, épica y legendaria) y su nivel. También sabemos el elixir medio usado por cada jugador y el momento en el que comenzo la partida. Además de todos estos datos, hay más información disponible en el dataset que no hemos utilizado, como puede ser la cantidad de coronas con la que empezado cada jugador la partida o el número de coronas que ha ganado el vencedor

## Descripción de la aplicación, modelos de programa, plataforma e infraestructura
Para el analisis de los datos, hemos creado diferentes scripts con Pyspark ejecutados en un Cluster de Google Cloud para los distintos estudios, además de una lambda función que se ejecuta para crear gráficas cuando la tarea de Pyspark finaliza gracias a la libreria de Python Matplotlib. Los datasets con la información y los resultados los guardamos en distintos Buckets de Google Cloud Storage. Junto con esto, hemos subido análisis de las gráficas en una página web alocada en GitHub Pages.

## Diseño de Software
Los distintos script que hemos usado se pueden encontrar en este repositorio. A continuación, vamos a explicar que hemos hecho en cada uno:

- [Top 100 Players with Most Wins](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-top100withMostWins.py): Para este estudio, hemos agrupado los datasets por id del jugador ganador en cada partida y hemos contado el número de apariciones de cada uno. Por último, hemos filtrado el resultado para quedarnos con el top 100.
- [Number of games played in each hour](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-numGamesInEveryHour.py): Primero configuramos la zona horaria de la ejecución de Pyspark para evitar problemas con los datos horarios, ya que al tener una zona horaria en la columna queremos que sean la misma. Despues extraemos la hora de la columna y agrupamos por esta. También filtramos para eliminar algunso elementos inválidos del dataset y finalizamos contando el número de partidas en cada hora.
- [Number of games in each range of average elixir used ingame](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-numGamesInEachRangesOfAverageExilir.py): Con este script pretendemos conocer cual es el número de partidas que hay en cada rango de elixir medio usado tanto por el jugador ganador como el perdedor. Como tenemos la media de cada uno, estudiamos las medias de los vencedores por un lado y la de los perdedores por otro, filtrando los elementos inválidos, asignadole el rango que corresponde a cada partida con una UDF y por último contando el cantidad de partidas en cada rango. Para finalizar unimos ambas tablas.
- [Number of decks with each card and its average level](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-numDecksWithEachCardAndAverageLevel.py): Vamos a estudiar cuantos veces esta presente cada carta del juego en los mazos y cual es su nivel medio. Para ello, debemos recolectar la información de las 32 distintas columnas que tienen la información del mazo de cada jugador. Primero creamos una array con pares de id de la carta y su nivel, para despues crear una columna con cada elemento del array. Con esta información agrupamos por id de la carta y hacemos dos funciones agregadas, contar el numero de veces que esta cada id en la tabla y la media del nivel. Despues unimos con la tabla que contiene los nombres de cada carta para que el resultado sea más legible.
- [Check the most played type of card in each game](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-checkMostPlayedTypeOfCardInEachGame.py): Queremos analizar cual es el tipo de carta (unidad, estructura o hechizo) que hay más presente en los distintos mazos del dataset. Para esto seleccionamos las columnas que contiene la información de la cantidad de cada tipo de carta presente en el mazo de cada jugador, y usamos una UDF para ver que etiqueta asignarle en caso de cual es mayor o si hay empate. Despues agrupamos y contamos el numero de partidas en las que predomina cada tipo de carta. 

Para crear la gráficas de los csv que devuelve todos estos scripts hemos desarrollado el siguiente [código](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/function/main.py) para crear una lambda función que se ejecute cada vez que una tarea escribe en el bucket de resultados.

## Uso
## Evaluación de desempeño
A continuación vamos a analizar el rendimiento de distintos cluster ejecutando los diferentes script que hemos desarrollado:

![img](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/imgs/compareCluster.png)

Para el mismo número de vCPUs, se puede observar que los cluster con 2 maquinas tienen menor tiempo de ejecución que los cluster con 4 maquinas (2 maquinas de 4 cores vs 4 maquinas de 2 cores y 2 maquinas de 8 cores vs 4 maquinas de 4 cores)

También podemos observar que el script que más tiempo tarda en ejecutarse es [Number of games in each range of average elixir used ingame](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-numGamesInEachRangesOfAverageExilir.py). Esto se puede deber al uso de funciones definidas por el desarrollador en vez de usar funciones nativas de Pyspark.

## Funcionalidades avanzadas
En el script de [Number of decks with each card and its average level](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-numDecksWithEachCardAndAverageLevel.py) hemos usado dos funciones que no hemos visto en clase: struct, que crea una nueva columna de estructuras a partir de las columnas que recibe, y array, que crea una nueva columna de un array a partir de los elementos de las columnas pasadas como parámetros.

Tambien hemos investigado a cerca de la posibilidad de definir funciones personalizadas por el desarrolador y el impacto de usar estas frente a aquellas que proporciona Pyspark:

![img](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/imgs/analyzeUDF.png)

Como se puede observar en la gráfica, el script que usa la UDF tiene un tiempo de ejecución mayor al que usa funciones de Pyspark para lograr el mismo objetivo.

Tras este analisis, podemos concluir que en caso de ser posible, es mejor usar métodos del propio Pyspark que andar desarrolando un código que logre el mismo objetivo.
## Conclusiones
## Referencias
- [Pyspark UDF](https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/)
- [Función struct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.struct.html)
- [Función array](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array.html)
