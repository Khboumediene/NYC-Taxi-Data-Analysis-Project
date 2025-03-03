import pandas as pd
import geopandas as gpd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

# Importer les données
trip_df = pd.read_parquet("./tlc_data/yellow_tripdata_2019-01.parquet")

# Filtrage des données
trip_df = trip_df[(trip_df["trip_distance"] <= 100) &
                  ~trip_df['PULocationID'].isin([264, 265]) &
                  ~trip_df['DOLocationID'].isin([264, 265])]

# Importer et préparer les zones
location_df = pd.read_csv("./data/taxi_zones/taxi_zone_lookup.csv")
location_geom_df = gpd.read_file("./data/taxi_zones/taxi_zones.shp")
location_geom_df["geometry"] = location_geom_df["geometry"].to_crs(epsg=4326)
location_geom_df["centroid"] = location_geom_df["geometry"].centroid

location_dim = location_df.copy()
location_dim["centroid_lat"] = location_geom_df["centroid"].apply(lambda p: p.y)
location_dim["centroid_long"] = location_geom_df["centroid"].apply(lambda p: p.x)
location_dim.columns = location_dim.columns.str.replace(' ', '_').str.lower()
location_dim.rename(columns={"locationid": "location_id"}, inplace=True)

# Fusionner avec les coordonnées
trip_df = trip_df.merge(location_dim[['location_id', 'centroid_lat', 'centroid_long']],
                        left_on='PULocationID', right_on='location_id', how='left') \
                 .rename(columns={'centroid_lat': 'PU_centroid_lat', 'centroid_long': 'PU_centroid_long'}) \
                 .drop(columns=['location_id'])

trip_df = trip_df.merge(location_dim[['location_id', 'centroid_lat', 'centroid_long']],
                        left_on='DOLocationID', right_on='location_id', how='left') \
                 .rename(columns={'centroid_lat': 'DO_centroid_lat', 'centroid_long': 'DO_centroid_long'}) \
                 .drop(columns=['location_id'])

# Dictionnaires de dimensions
rate_code_type = {1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau or Westchester",
                  5: "Negotiated fare", 6: "Group ride", 99: "Undefined"}
rate_code_dim = pd.DataFrame(list(rate_code_type.items()), columns=['rate_code_id', 'rate_code_name'])

payment_type_name = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}
payment_type_dim = pd.DataFrame(list(payment_type_name.items()), columns=['payment_type_id', 'payment_type_name'])

trip_df = trip_df.merge(rate_code_dim, left_on='RatecodeID', right_on='rate_code_id', how='left') \
                 .drop(columns=['rate_code_id'])
trip_df = trip_df.merge(payment_type_dim, left_on='payment_type', right_on='payment_type_id', how='left') \
                 .drop(columns=['payment_type_id'])

# Ne garder que les colonnes nécessaires
columns_to_keep = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
                   "RatecodeID", "rate_code_name", "payment_type", "payment_type_name", "PULocationID",
                   "DOLocationID", "PU_centroid_lat", "PU_centroid_long", "DO_centroid_lat", "DO_centroid_long",
                   "total_amount"]
trip_df = trip_df[columns_to_keep]

# Supprimer les lignes contenant des valeurs nulles
trip_df = trip_df.dropna()

# Connexion à Elasticsearch
es = Elasticsearch("http://localhost:9200", basic_auth=("elastic", "RjYhZdoNKUWyyKQCjP8G"))
index_name = "nyc_yellow_taxi_trips"

def index_data(df, index_name):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {col: {"type": "float"} if df[col].dtype == "float64" else {"type": "integer"} 
                               if df[col].dtype == "int64" else {"type": "date" if 'datetime' in col else "keyword"}
                               for col in df.columns}
            }
        })
    
    def generate_data(df):
        for _, row in df.iterrows():
            doc = row.to_dict()
            doc["tpep_pickup_datetime"] = doc["tpep_pickup_datetime"].isoformat()
            doc["tpep_dropoff_datetime"] = doc["tpep_dropoff_datetime"].isoformat()
            yield {"_index": index_name, "_source": doc}
    
    try:
        success, failed = bulk(es, generate_data(df), stats_only=True)
        print(f"✅ Indexé avec succès: {success}, Échecs: {failed}")
    except BulkIndexError as e:
        print(f"Erreur lors de l'indexation: {e}")

print(f"📦 Stockage des données dans l'index {index_name}...")
index_data(trip_df, index_name)