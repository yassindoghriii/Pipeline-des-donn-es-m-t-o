import os
import json
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch, helpers

# Configuration des brokers Kafka et de l'hôte Elasticsearch
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'http://localhost:9200')

# Initialisation du consommateur Kafka
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'weather-consumer-group',
    'auto.offset.reset': 'earliest'
})

# S'abonner au topic 'weather'
consumer.subscribe(['weather'])
print("Consommateur démarré, en attente de messages...")

# Connexion à Elasticsearch avec vérification
try:
    es = Elasticsearch(ELASTICSEARCH_HOST)
    if not es.ping():
        print("Échec de la connexion à Elasticsearch")
        exit(1)
    print("Connecté à Elasticsearch")
except Exception as e:
    print(f"Erreur de connexion à Elasticsearch: {e}")
    exit(1)

def index_to_elasticsearch(data):
    """Indexe les données dans Elasticsearch"""
    try:
        # Utiliser un champ unique (par exemple, city et timestamp) pour éviter les doublons
        doc_id = f"{data['city']}-{data['timestamp']}"
        es.index(index='weather_data', id=doc_id, body=data)
        print(f"Données indexées dans Elasticsearch: {data}")
    except Exception as e:
        print(f"Erreur lors de l'indexation des données : {e}")

if __name__ == "__main__":
    try:
        while True:
            # Récupérer un message de Kafka
            msg = consumer.poll(1.0)
            if msg is None:  # Pas de message reçu
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Erreur du consommateur: {msg.error()}")
                continue

            # Afficher le message reçu
            print(f"Message reçu: {msg.value().decode('utf-8')}")

            try:
                # Désérialiser le message JSON
                data = json.loads(msg.value().decode('utf-8'))
                # Indexer les données dans Elasticsearch
                index_to_elasticsearch(data)
            except json.JSONDecodeError as e:
                print(f"Erreur de décodage JSON: {e}")
    except KeyboardInterrupt:
        print("Consommateur arrêté par l'utilisateur.")
    finally:
        consumer.close()
        print("Consommateur Kafka fermé.")