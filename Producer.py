from confluent_kafka import Producer
import json
import time
from faker import Faker
from DataGeneration import generate_cdr

# Initialisation de Faker
fake = Faker()

# Configuration Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'retries': 3,
    'linger.ms': 5,
    'batch.size': 32768,
    'compression.type': 'snappy',
    'enable.idempotence': True,
    'client.id': 'cdr-producer',
    'max.in.flight.requests.per.connection': 5
}

# Création du producteur Kafka
producer = Producer(**conf)

# Callback de confirmation ou d'erreur
def delivery_report(err, msg):
    if err is not None:
        print(f"[ERREUR] Échec d'envoi : {err}")
    else:
        print(f"[OK] Message envoyé à {msg.topic()} [{msg.partition()}]")

# Configuration
topic = "telecom"  
interval = 1.2

# Boucle principale d'envoi
try:
    while True:
        for _ in range(3):  # Moins de messages par lot
            cdr = generate_cdr()
            key = str(fake.random_int(1, 1000))

            producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(cdr),
                callback=delivery_report
            )
            producer.poll(0)  # Traite les callbacks en attente

        time.sleep(interval)
except KeyboardInterrupt:
    print("\n⛔ Interruption manuelle détectée.")
finally:
    producer.flush()
    print("✅ Producteur Kafka fermé proprement.")
