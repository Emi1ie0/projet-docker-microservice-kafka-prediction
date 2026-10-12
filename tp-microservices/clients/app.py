from flask import Flask, jsonify

app = Flask(__name__)

clients = [
    {"id":1, "nom": "Alice", "pays": "France"},
    {"id":2, "nom": "Alex", "pays": "Belgique"},
    {"id":3, "nom": "Madeleine", "pays": "France"}
]

#1.3 : GET /clients : retourne tous les clients
@app.route("/clients")
def deg_clients():
    return jsonify(clients)

#1.3 : GET /clients/<id> : retourne un client
@app.route("/clients/<int:id>")
def deg_client(id):
    for client in clients:
        if client["id"]== id:
            return jsonify(client)
        
    # 2.2 gestion des erreurs : client inexistant
    return jsonify({"erreur": "Client introuvable"}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port =8080)