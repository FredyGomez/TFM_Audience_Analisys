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
Este componente carga el modelo principal de datos en BigQuery desde tablas externas aplicando pequeñas transformaciones.


data_preparation_denormalization.sql
Este segundo componente desnormaliza partiendo del modelo anterior y algunas tablas estáticas como country, utiliza un proceso staging inicial y luego la desnormalización final, también prepara algunos campos para un uso mas sencillo, como las categorías, variables y subvariables, codes, etc.

data_viz_preparation.sql
Este último paso genera un esquema preparado y optimizado para visualizaci´on, aunque ya en el modelo anterior se conseguían resultados suficientes, este adeás nos sirve para mejorar las relaciones entre datos e incluso cargar el modelo en el mismo PowerBI. Además de hacer algunas transformaciones ad-hoc para un caso de uso basado en fabricantes de automóviles.




## Anesrake algoritmo en R - Notebooks

Notebook que, recibiendo tablas disponibles en BigQuery, accede a las mismas y ejecuta el algoritmo anesrake
01_r_anesrake_compute.ipynb

Notebook utilizado para validar datos, solo a modo de exploración, no necesario en el cálculo.
02_r_anesrake_validate.ipynb



## Visualización desarrollada en PowerBI


Dashboard en formato pbix: https://storage.googleapis.com/tfm_audiences/Panel_Audiences.pbix


## Ejemplos de datos de entrada

profiles_fr_2021_12_05.default_ns_n3.en.codes.mr.csv.gz: Codes entity en inglés y csv 
profiles_fr_2021_12_05.default_ns_n3.en.definitions.jsonl.gz: Definitions entity en ingles y json
profiles_fr_2021_12_05.default_ns_n3.en.definitions.mr.csv.gz: Definitions entity en inglés y csv
profiles_fr_2021_12_05.default_ns_n3.en.subvariables.mr.csv.gz: Subvariables entity en inglés y csv
profiles_fr_2021_12_05.default_ns_n3.fr.codes.mr.csv.gz: Codes entity en francés y csv
profiles_fr_2021_12_05.default_ns_n3.fr.definitions.jsonl.gz: Definitions entity en francés y json
profiles_fr_2021_12_05.default_ns_n3.fr.definitions.mr.csv.gz: Definitions entity en francés y csv

profiles_fr_2021_12_05.default_ns_n3.fr.subvariables.mr.csv.gz
