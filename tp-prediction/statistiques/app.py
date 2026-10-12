import os
import json
import threading
from flask import Flask, jsonify
from confluent_kafka import Consumer

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "commandes-creees"

# 1.7 : nombre de commandes par produit (produit -> total)
commandes_par_produit = {}
# 1.7 : chiffre d'affaires par mois (AAAA-MM -> montant total)
ca_par_mois = {}

def consommer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "statistiques-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Erreur Kafka :", msg.error(), flush=True)
            continue

        evenement = json.loads(msg.value().decode("utf-8"))

        # nombre de commandes par produit
        produit = evenement["produit"]
        commandes_par_produit[produit] = commandes_par_produit.get(produit, 0) + 1

        # chiffre d'affaires par mois (seulement si l'événement a une date)
        if "date" in evenement:
            mois = evenement["date"][:7]  # "2026-10-12" -> "2026-10"
            ca_par_mois[mois] = ca_par_mois.get(mois, 0) + evenement["montant"]

        print(f"Commande reçue : produit={produit}, montant={evenement['montant']}, "
              f"date={evenement.get('date', 'aucune')}", flush=True)

# 1.7 : nombre total de commandes pour un produit donné
@app.route("/statistiques/produits/<produit>")
def stats_produit(produit):
    return jsonify({
        "produit": produit,
        "nombre_commandes": commandes_par_produit.get(produit, 0),
    })

# 1.7 : chiffre d'affaires total pour un mois donné (format AAAA-MM)
@app.route("/statistiques/mois/<mois>")
def stats_mois(mois):
    return jsonify({
        "mois": mois,
        "chiffre_affaires": ca_par_mois.get(mois, 0),
    })

if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)