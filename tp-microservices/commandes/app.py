from flask import Flask, jsonify
import requests

app = Flask(__name__)

#les questions du 1.4:
#les questions du 1.5 : communication entre micro services sont mis en commentaire, avec #1.5.1, #1.5.2...
# notre but est que le micro service commande puisse communiquer avec le micro service client
# Soit depuis la commande 1 par exemple, récupérer les infos associé au client de cette commande

#des commentaires ont aussi été rajoutées au dessus de la gestion des erreurs demandées en 2.2

#clients = nom du conteneur clients sur le réseau Docker
CLIENTS_URL = "http://clients:8080"


commandes = [
    {"id":1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id":2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id":3, "client_id": 1, "produit": "Casque", "montant": 80}
]

#1.4 : GET /commandes : retourne toutes les commandes
@app.route("/commandes")
def deg_commandes():
    return jsonify(commandes)

#1.4 : GET /commandes/<id> : retourne une commande (ajout du 1.5 plus les infos du client)
@app.route("/commandes/<int:id>")
def deg_commande(id):

    # 1.5.1 récuperer commande 
    commande = None

    for c in commandes :
    
        if c["id"]== id:
            commande = c
    
    #2.2 gestion des erreurs : commande inexistante
    if commande is None:
        return jsonify({"erreur": "Commande introuvable"}), 404
    
    #1.5.2 récuperer client id pour #1.5.3 appeler le microservice clients avec et #1.5.4 récupérer les infos du client
    try:
        r = requests.get(f"{CLIENTS_URL}/clients/{commande['client_id']}", timeout=3)

    #2.2 gestion des erreurs : microservice clients indisponible  
    except requests.exceptions.RequestException:
        return jsonify({"erreur": "Service clients indisponible"}), 503
    
    #2.2 gestion des erreurs : client inexistant
    if r.status_code == 404:
        return jsonify({"erreur": "Client introuvable"}), 404
    
    # 1.5.5 renvoyer commande avec les infos du client 
    return jsonify({
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": r.json(),

    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port =8080)