import os
import json
import time
import requests
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, avg, when
 
# Configuration de Kafka et de l'API météo
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
API_KEY = os.getenv('WEATHER_API_KEY', '528f728527f79551dd8746c86fd00b6c')
CITIES = ["Marseille", "Lyon", "Nice", "Bordeaux", "Toulouse", "Lille", "Nantes", "Strasbourg"]
 
def get_weather_data(city):
    """Récupère les données météo depuis l'API OpenWeatherMap pour une ville donnée"""
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return {
            "city": data['name'],
            "temperature": data['main']['temp'],
            "humidity": data['main']['humidity'],
            "pressure": data['main']['pressure'],
            "wind_speed": data['wind']['speed'],
            "description": data['weather'][0]['description'],
            "latitude": data['coord']['lat'],
            "longitude": data['coord']['lon'],
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        print(f"Erreur lors de la récupération des données pour {city}: {e}")
        return None
 
def delivery_report(err, msg):
    """Rapporte le statut de livraison des messages Kafka"""
    if err is not None:
        print(f"Erreur lors de l'envoi du message : {err}")
    else:
        print(f"Message envoyé : {msg.value()} au topic {msg.topic()}")
 
# Configuration du producteur Kafka
producer = Producer({'bootstrap.servers': KAFKA_BROKER})
 
def produce_weather_data():
    """Envoie les données météo à Kafka"""
    for city in CITIES:
        weather_data = get_weather_data(city)
        if weather_data:
            try:
                data_str = json.dumps(weather_data)
                producer.produce('weather', value=data_str, callback=delivery_report)
                producer.flush()
                print(f"Données envoyées pour {city} : {data_str}")
            except Exception as e:
                print(f"Erreur lors de l'envoi des données pour {city}: {e}")
 
# Configuration de Spark
spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .getOrCreate()
 
def process_weather_data():
    """Traite les données météo avec PySpark et fournit des recommandations"""
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("temperature", FloatType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("description", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("timestamp", StringType(), True)
    ])
 
    # Lire les données depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "weather") \
        .load()
 
    # Transformation des données
    weather_df = df.selectExpr("CAST(value AS STRING)") \
        .selectExpr("CAST(value AS STRING) as json") \
        .selectExpr("from_json(json, schema) as data") \
        .select("data.*")
 
    # Ajout de recommandations d'arrosage
    enriched_df = weather_df.withColumn(
        "watering_advice",
        when(
            (col("temperature") > 25) & (col("humidity") < 50),
            "Arrosage recommandé (température élevée, faible humidité)"
        ).when(
            col("description").like("%rain%"),
            "Pas d'arrosage nécessaire (pluie prévue)"
        ).otherwise("Arrosage non nécessaire pour le moment")
    )
 
    # Analyse des données (par exemple : température moyenne par ville)
    average_temperature = enriched_df.groupBy("city") \
        .agg(avg("temperature").alias("avg_temperature"))
 
    # Écriture des résultats dans la console
    query1 = average_temperature.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
 
    # Écriture des recommandations dans la console
    query2 = enriched_df.select("city", "watering_advice", "timestamp").writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
 
    query1.awaitTermination()
    query2.awaitTermination()
 
if __name__ == "__main__":
    # Étape 1 : Production des données
    produce_weather_data()
 
    # Étape 2 : Traitement des données avec PySpark
    process_weather_data()
 