# TFM_Audience_Analisys

_En este repositorio se alojan las distintas piezas de código, pipelines y visualización; así como un conjunto de datos ejemplo del trabajo fin de master realizado (UNIR 2021-2022) por Alfredo Gómez Castillejos con título: Análisis de audiencias mediante datos de panelistas para optimización de campañas publicitarias_

El trabajo se compone de las siguientes piezas por cada componente utilizado

## Integración Continua de Datos Streamsets con las siguients Pipelines

### Carga de datos desde la fuente 

YOUGOV_PROFILES_METADATA_S3-GCS.zip
Pipeline en Streamsets que recupera continuamente los datos desde el directorio de metadatos, aplicando las reglas indicadas en la memoria para parsear y sobreescribir los datos en destino.

YOUGOV_PROFILES_RESPONSES_S3-GCS.zip
Pipeline en Streamsets que recupera continuamente los datos desde el directorio de datos, aplicando las reglas indicadas en la memoria para parsear y sobreescribir los datos en destino.

YOUGOV_PROFILES_FILE_COUNT.zip
Pipeline que se ejecuta para confirmar que han llegado todos los ficheros que deberían de llegar y validar la carga semanal de datos.


### Procesado de Datos

YOUGOV_PROFILES_DATAPREP_ANESRAKE_DATAVIZ.zip
Pipeline qhe orquesta todo el procesado de datos dentro de la plataforma
1) ejecuta las primeras queries de carga y preparación en BigQuery
2) ejecuta el proceso de Anesrake en Dataproc
3) termina el proceso de preparacion para visualizacion enBigQuery
4) refresca el dashboard utilizando el API de PowerBI




## Preparación de datos en Bigquery con las siguientes piezas de código SQL
data_preparation_initial.sql



data_preparation_denormalization.sql


data_viz_preparation.sql





## Anesrake algoritmo en R - Notebooks

Notebook que, recibiendo tablas disponibles en BigQuery, accede a las mismas y ejecuta el algoritmo anesrake
01_r_anesrake_compute.ipynb

Notebook utilizado para validar datos, solo a modo de exploración, no necesario en el cálculo.
02_r_anesrake_validate.ipynb



## Visualización desarrollada en PowerBI


Dashboard en formato pwbi


## Ejemplos de datos de entrada


