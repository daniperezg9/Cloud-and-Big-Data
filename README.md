# Cloud & Big Data - Proyecto Final
## Descripción del problema
El proyecto se centra en el análisis de partidas de la temporada 18 de Clash Royale, un popular juego de estrategia en tiempo real desarrollado por Supercell. Con información de unas aproximadamente 38 millones de partidas, ofrece una rica fuente de datos para estudiar diversos aspectos del comportamiento de los jugadores y las dinámicas del juego, como las cartas más jugadas o el uso de recursos (elixir).

Clash Royale enfrenta a dos jugadores en una arena, donde deben utilizar una combinación de cartas de tropas, hechizos y edificios para destruir las torres del oponente. Cada partida dura hasta 3 minutos, con tiempo extra si es necesario para determinar un ganador.

## Necesidad de procesamiento de Big Data y Cloud Computing
Clash Royale es un juego que cuenta con millones de usuarios activos jugando casi a diario, lo que hace necesario usar técnicas especializadas en el procesamiento masivo de datos. Cloud es una herramienta útil para el procesamiento de estos datos gracias a su capacidad de escalabilidad, flexibilidad y costos eficientes. Además, ofrece un rendimiento optimizado y seguridad robusta, que lo convierte en una solución ideal para manejar la inmensa cantidad de datos generado por los jugadores.

## Descripción de los datos
La información de las partidas se ha extraido de la siguiente página de datasets: [Partidas Clash Royale](https://www.kaggle.com/datasets/bwandowando/clash-royale-season-18-dec-0320-dataset). En ella se explica que los datos han sido extraídos mediante la API de Supercell para el juego. También se explica que la información recolectada, aunque pueda parecer una gran cantidad (38 millones de partidas, de las cuales nosotros hemos escogido la información de unas 17 millones), no es nada comparado con la cantidad total de partidas jugadas durante la temporada 18.

Los archivos se encuentran en el formato de csv. Cada archivo presenta la información de distintas partidas jugadas durante la temporada 18. Para cada partida se sabe el jugador vencedor y perdedor, el mazo que ha usado cada jugador, el número de cartas de cada tipo (tropas, estructuras, hechizos) que ha usado cada jugador, así como la calidad (común, rara, épica y legendaria) y su nivel. También sabemos el elixir medio usado por cada jugador y el momento en el que comenzó la partida. Además de todos estos datos, hay más información disponible en el dataset que no hemos utilizado, como puede ser la cantidad de coronas con la que ha empezado cada jugador la partida o el número de coronas que ha obtenido el vencedor.

## Descripción de la aplicación, modelos de programa, plataforma e infraestructura
Para el análisis de los datos, hemos creado diferentes scripts con Pyspark ejecutados en un Cluster de Google Cloud para los distintos estudios, además de una lambda función que se ejecuta para crear gráficas cuando la tarea de Pyspark finaliza gracias a la libreria de Python Matplotlib. Los datasets con la información y los resultados los guardamos en distintos Buckets de Google Cloud Storage. Junto con esto, hemos subido análisis de las gráficas en una página web alocada en GitHub Pages.

## Diseño de Software
Los distintos scripts que hemos usado se pueden encontrar en este repositorio. A continuación, vamos a explicar que hemos hecho en cada uno:

- [Top 100 Players with Most Wins](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-top100withMostWins.py): Para este estudio, hemos agrupado los datasets por id del jugador ganador en cada partida y hemos contado el número de apariciones de cada uno. Por último, hemos filtrado el resultado para quedarnos con el top 100.
- [Number of games played in each hour](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-numGamesInEveryHour.py): Primero configuramos la zona horaria de la ejecución de Pyspark para evitar problemas con los datos horarios, ya que al tener una zona horaria en la columna queremos que sean la misma. Después extraemos la hora de la columna y agrupamos por esta. También filtramos para eliminar algunos elementos inválidos del dataset y finalizamos contando el número de partidas en cada hora.
- [Number of games in each range of average elixir used ingame](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-numGamesInEachRangesOfAverageExilir.py): Con este script pretendemos conocer cuál es el número de partidas que hay en cada rango de elixir medio usado tanto por el jugador ganador como el perdedor. Como tenemos la media de cada uno, estudiamos las medias de los vencedores por un lado y la de los perdedores por otro, filtrando los elementos inválidos, asignándole el rango que corresponde a cada partida con una UDF y por último contando la cantidad de partidas en cada rango. Para finalizar unimos ambas tablas.
- [Number of decks with each card and its average level](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-numDecksWithEachCardAndAverageLevel.py): Vamos a estudiar cuantos veces esta presente cada carta del juego en los mazos y cual es su nivel medio. Para ello, debemos recolectar la información de las 32 distintas columnas que tienen la información del mazo de cada jugador. Primero creamos un array con pares de id de la carta y su nivel, para después crear una columna con cada elemento del array. Con esta información agrupamos por id de la carta y hacemos dos funciones agregadas, contar el número de veces que está cada id en la tabla y la media del nivel. Después unimos con la tabla que contiene los nombres de cada carta para que el resultado sea más legible.
- [Check the most played type of card in each game](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/clash-royale-checkMostPlayedTypeOfCardInEachGame.py): Queremos analizar cuál es el tipo de carta (unidad, estructura o hechizo) que hay más presente en los distintos mazos del dataset. Para eso seleccionamos las columnas que contiene la información de la cantidad de cada tipo de carta presente en el mazo de cada jugador, y usamos una UDF para ver que etiqueta asignarle en caso de cuál es mayor o si hay empate. Después agrupamos y contamos el número de partidas en las que predomina cada tipo de carta. 

Para crear la gráficas de los csv que devuelven todos estos scripts hemos desarrollado el siguiente [código](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/Code/function/main.py) para crear una lambda función que se ejecute cada vez que una tarea escribe en el bucket de resultados.

## Uso
Lo primero de todo es crear la lamba función, para que esta se ejecute al finalizar las tareas de los cluster. Para ello dentro de Google Cloud Console iremos a Cloud Run Functions y le daremos a crear función. Dejamos el environment por defecto y le ponemos un nombre descriptivo a la función (ej: generateDiagramFromCsv). Debemos seleccionar la región para que sea la misma que la del bucket donde vayamos a guardar los resultados. Configuramos el trigger seleccionando el tipo Google Storage y tipo de evento google.cloud.storage.object.v1.finalized. También debemos introducir el bucket del cual estará pendiente la función: [heroic-muse-436812-j2-result](https://console.cloud.google.com/storage/browser/heroic-muse-436812-j2-result). También es necesario aumentar la memoria alocada a 512Mib, pues en caso contrario algunas de las gráficas no se generan al quedarse sin memoria. 

![img](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/imgs/lambda_1.png)

Por último, pulsamos el boton de siguiente, seleccionamos el runtime a Python 3.10 y subimos el main.py y requirements.txt. Ponemos el punto de entrada a la función gcs y desplegamos.

![img](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/imgs/lambda_2.png)

Ahora debemos crear el cluster donde ejecutaremos los diferentes scripts. Aquí están los códigos a ejecutar en el shell para crear los diferentes cluster. Es importante crear uno, ejecutar todos los scripts y despues crear otro, ya que Google Cloud puede limitar el número de vCPUs que se pueden usar simultáneamente (en nuestro caso eran 24).

- Cluster con solo el nodo maestro:

  gcloud dataproc clusters create masteronly --region=europe-southwest1 \\ \
--master-machine-type=e2-standard-4 --master-boot-disk-size=50 \\ \
--single-node --enable-component-gateway
  
- Cluster con 2 trabajadores de 4 vCPUs:

  gcloud dataproc clusters create fourcorestwomachines --region=europe-southwest1 \\ \
--master-machine-type=e2-standard-4 --master-boot-disk-size=50 \\ \
--worker-machine-type=e2-standard-4 --worker-boot-disk-size=50 \\ \
--enable-component-gateway
  
- Cluster con 2 trabajadores de 8 vCPUs:

  gcloud dataproc clusters create eightcorestwomachines --region=europe-southwest1 \\ \
--master-machine-type=e2-standard-4 --master-boot-disk-size=50 \\ \
--worker-machine-type=e2-standard-8 --worker-boot-disk-size=50 \\ \
--enable-component-gateway
  
- Cluster con 4 trabajadores de 2 vCPUs:

  gcloud dataproc clusters create twocoresfourmachines --region=europe-southwest1 \\ \
--master-machine-type=e2-standard-4 --master-boot-disk-size=50 \\ \
--worker-machine-type=e2-standard-2 --worker-boot-disk-size=50 \\ \
--num-workers=4 --enable-component-gateway
  
- Cluster con 4 trabajadores de 4 vCPUs:

  gcloud dataproc clusters create fourcoresfourmachines --region=europe-southwest1 \\ \
--master-machine-type=e2-standard-4 --master-boot-disk-size=50 \\ \
--worker-machine-type=e2-standard-4 --worker-boot-disk-size=50 \\ \
--num-workers=4 --enable-component-gateway

![img](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/imgs/cluster_1.png)

Una vez creado el cluster, debemos ejecutar los diferentes scripts

INPUT_BUCKET=gs://heroic-muse-436812-j2/project

OUTPUT_BUCKET=gs://heroic-muse-436812-j2-result

gcloud dataproc jobs submit pyspark -- cluster [cluster_name] --region=europe-southwest1 [script] -- \\ \
$INPUT_BUCKET/datasets $OUTPUT_BUCKET [number of virtual cores]

![img](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/imgs/cluster_2.png)

Una vez que finalicen, impriran en la pantalla del Cloud Shell el tiempo de ejecución, que lo anotaremos para un análisis posterior

![img](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/imgs/cluster_3.png)

Por último, podemos ver que en el bucket de resultados se ha creado un diagrama en la carpeta diagrams

![img](https://github.com/daniperezg9/Cloud-and-Big-Data/blob/main/imgs/bucket_1.png)

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

Tras este análisis, podemos concluir que en caso de ser posible, es mejor usar métodos del propio Pyspark que andar desarrolando un código que logre el mismo objetivo.
## Conclusiones
## Referencias
- [Pyspark UDF](https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/)
- [Función struct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.struct.html)
- [Función array](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array.html)
