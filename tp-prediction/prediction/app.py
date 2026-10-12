import os
import json
import socket
import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_RESULTATS = "resultats-prediction"
INSTANCE = socket.gethostname()  # nom de la machine / du conteneur (utile en 3.7)

# 3.1 : chargement du modèle déjà entraîné, une seule fois au démarrage
model = joblib.load("modele_commande.pkl")

# consommateur des demandes de prédiction
consumer = KafkaConsumer(
    TOPIC_DEMANDES,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="prediction-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

# producteur des résultats
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"[{INSTANCE}] Modèle chargé, en attente de demandes...", flush=True)

for msg in consumer:
    data = msg.value
    try:
        # 3.4 : construction de l'entrée du modèle
        X = pd.DataFrame([{
            "age": data["age"],
            "anciennete": data["anciennete"],
            "nombre_commandes": data["nombre_commandes"],
            "montant_total": data["montant_total"],
            "montant_moyen": data["montant_moyen"],
            "jours_depuis_derniere_commande": data["jours_depuis_derniere_commande"],
        }])

        # 3.5 : prédiction et probabilité
        prediction = model.predict(X)[0]
        probabilite = model.predict_proba(X)[0][1]

        print(f"[{INSTANCE}] partition {msg.partition}", flush=True)
        print("Client :", data["client_id"], flush=True)
        print("Prediction :", prediction, flush=True)
        print("Probabilite :", probabilite, flush=True)

        # 3.6 : événement de prédiction, envoyé à commandes
        resultat = {
            "client_id": data["client_id"],
            "prediction": int(prediction),
            "probabilite_commande": float(probabilite),
        }
        producer.send(TOPIC_RESULTATS, resultat)
        producer.flush()
    except KeyError as e:
        # demande incomplète : on l'ignore au lieu de planter le service
        print(f"[{INSTANCE}] Demande ignorée, champ manquant : {e}", flush=True)