import os
import json
from flask import Flask, jsonify, request
import threading
from confluent_kafka import Producer, Consumer
from datetime import date

app = Flask(__name__)

# 1.4 : connexion du producteur au broker Kafka
# localhost:9092 depuis l'ordinateur, kafka:29092 depuis un conteneur
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# 1.4 : topic Kafka utilisé
TOPIC = "commandes-creees"
# 3.3 et 3.6 : topics de la prédiction
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_RESULTATS = "resultats-prediction"

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80},
]

# GET /commandes : retourne toutes les commandes
@app.route("/commandes")
def get_commandes():
    return jsonify(commandes)

# GET /commandes/<id> : retourne une commande
@app.route("/commandes/<int:id>")
def get_commande(id):
    for commande in commandes:
        if commande["id"] == id:
            return jsonify(commande)
    return jsonify({"erreur": "Commande introuvable"}), 404

# 1.4 : POST /commandes : crée une commande et publie un événement dans Kafka
@app.route("/commandes", methods=["POST"])
def creer_commande():
    data = request.get_json()

    nouvelle = {
        "id": max(c["id"] for c in commandes) + 1,
        "client_id": data["client_id"],
        "produit": data["produit"],
        "montant": data["montant"],
        # 1.7 : date de la commande (aujourd'hui par défaut)
        "date": data.get("date", date.today().isoformat()),
    }
    commandes.append(nouvelle)

    # 1.4 : structure du message JSON envoyé
    evenement = {
        "commande_id": nouvelle["id"],
        "client_id": nouvelle["client_id"],
        "produit": nouvelle["produit"],
        "montant": nouvelle["montant"],
        "date": nouvelle["date"],
    }

    # 1.4 : envoi du message dans le topic
    producer.produce(TOPIC, value=json.dumps(evenement).encode("utf-8"))
    producer.flush()

    return jsonify(nouvelle), 201

# 3.3 : POST /predictions : publie une demande de prédiction
@app.route("/predictions", methods=["POST"])
def demander_prediction():
    data = request.get_json()
    producer.produce(TOPIC_DEMANDES, value=json.dumps(data).encode("utf-8"))
    producer.flush()
    return jsonify({"message": "Demande de prédiction envoyée", "client_id": data["client_id"]}), 202

# 3.6 : lecture et affichage des résultats de prédiction
def lire_resultats():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "commandes-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC_RESULTATS])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Erreur Kafka :", msg.error(), flush=True)
            continue
        r = json.loads(msg.value().decode("utf-8"))
        print(f"Résultat de prédiction : client={r['client_id']}, "
              f"prediction={r['prediction']}, probabilite={r['probabilite_commande']:.2f}", flush=True)


if __name__ == "__main__":
    # 3.6 : écoute des résultats en parallèle du serveur Flask
    threading.Thread(target=lire_resultats, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)