import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from elasticsearch import Elasticsearch

sys.stdout.reconfigure(encoding='utf-8')

# Connexion à Elasticsearch
es = Elasticsearch("http://localhost:9200", basic_auth=("elastic", "RjYhZdoNKUWyyKQCjP8G"))

index_name = "nyc_taxi_analytics"

# Création de l'index si nécessaire
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)
    print(f"Index {index_name} créé avec succès.")

# Connexion à Spark
spark = SparkSession.builder \
    .appName("NYC Taxi Analysis") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.es.net.http.auth.user", "elastic") \
    .config("spark.es.net.http.auth.pass", "RjYhZdoNKUWyyKQCjP8G") \
    .getOrCreate()

df = spark.read.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "nyc_yellow_taxi_trips") \
    .option("es.read.metadata", "false") \
    .load()

df.createOrReplaceTempView("trips")

output_index = "nyc_taxi_analytics"

# Fonction pour ajouter un timestamp et un type de statistique
def save_statistic(df, stat_name):
    df = df.withColumn("timestamp", current_timestamp()).withColumn("stat_name", lit(stat_name))
    df.show()
    df.write.format("org.elasticsearch.spark.sql").mode("append").option("es.resource", output_index).save()

# ✅ Ajout des statistiques avec timestamp et nom du document
save_statistic(spark.sql("SELECT SUM(total_amount) AS value FROM trips"), "total_revenue")
save_statistic(spark.sql("SELECT COUNT(*) AS value FROM trips"), "number_of_trips")
save_statistic(spark.sql("SELECT AVG(total_amount) AS value FROM trips"), "average_fare")
save_statistic(spark.sql("SELECT AVG(trip_distance) AS value FROM trips"), "average_distance")

save_statistic(spark.sql("""
    SELECT rate_code_name AS category, SUM(total_amount) AS value 
    FROM trips 
    GROUP BY rate_code_name
"""), "revenue_per_rate_code")

save_statistic(spark.sql("""
    SELECT payment_type_name AS category, SUM(total_amount) AS value 
    FROM trips 
    GROUP BY payment_type_name
"""), "revenue_per_payment_type")

save_statistic(spark.sql("""
    SELECT passenger_count AS category, SUM(total_amount) AS value 
    FROM trips 
    GROUP BY passenger_count
"""), "revenue_per_passenger")

save_statistic(spark.sql("""
    SELECT passenger_count AS category, COUNT(*) AS value 
    FROM trips 
    GROUP BY passenger_count
"""), "trips_per_passenger")

save_statistic(spark.sql("""
    SELECT HOUR(tpep_pickup_datetime) AS category, COUNT(*) AS value 
    FROM trips 
    GROUP BY pickup_hour 
    ORDER BY pickup_hour
"""), "trips_by_hour")

save_statistic(spark.sql("""
    SELECT PULocationID AS group_by_value, COUNT(*) AS value 
    FROM trips 
    GROUP BY PULocationID 
    ORDER BY value DESC
"""), "trips_by_pickup_location")

spark.stop()
print("Analyse terminée et résultats ajoutés dans Elasticsearch !")
