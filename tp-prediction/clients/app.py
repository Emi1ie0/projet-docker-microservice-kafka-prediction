import os
import json
import threading
from flask import Flask, jsonify
from confluent_kafka import Consumer

app = Flask(__name__)

# 1.5 : connexion au broker Kafka et topic écouté
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "commandes-creees"

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"},
]

# 1.5 : nombre total de commandes par client (client_id -> total)
nb_commandes = {}

def trouver_client(id):
    for client in clients:
        if client["id"] == id:
            return client
    return None

def nom_client(id):
    client = trouver_client(id)
    if client is None:
        return f"client inconnu (id {id})"
    return client["nom"]

# 1.5 : consommation des événements commandes-creees
def consommer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": os.environ.get("GROUP_ID", "clients-group"),
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    while True:
        msg = consumer.poll(1.0)  # attend un message pendant 1 seconde
        if msg is None:
            continue
        if msg.error():
            print("Erreur Kafka :", msg.error(), flush=True)
            continue

        evenement = json.loads(msg.value().decode("utf-8"))
        client_id = evenement["client_id"]

        # mise à jour du compteur
        nb_commandes[client_id] = nb_commandes.get(client_id, 0) + 1

        # affichage de la commande reçue
        print(f"Commande reçue : client={nom_client(client_id)}, "
              f"produit={evenement['produit']}, montant={evenement['montant']}", flush=True)

        # affichage du total par client
        print("Total des commandes par client :", flush=True)
        for cid, total in nb_commandes.items():
            print(f"  - {nom_client(cid)} : {total}", flush=True)

# API REST du TP1 (inchangée)
@app.route("/clients")
def get_clients():
    return jsonify(clients)

@app.route("/clients/<int:id>")
def get_client(id):
    client = trouver_client(id)
    if client is None:
        return jsonify({"erreur": "Client introuvable"}), 404
    return jsonify(client)

if __name__ == "__main__":
    # lance l'écoute de Kafka en parallèle du serveur Flask
    threading.Thread(target=consommer, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)