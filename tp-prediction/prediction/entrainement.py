import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# 3.1 : données historiques simulées (le sujet n'en fournit pas)
rng = np.random.default_rng(42)
n = 1000

df = pd.DataFrame({
    "age": rng.integers(18, 70, n),
    "anciennete": rng.integers(0, 10, n),
    "nombre_commandes": rng.integers(0, 30, n),
    "jours_depuis_derniere_commande": rng.integers(0, 365, n),
})
df["montant_moyen"] = rng.uniform(10, 300, n).round(2)
df["montant_total"] = (df["nombre_commandes"] * df["montant_moyen"]).round(2)

# règle inventée : un client fidèle et récent commande plus souvent (+ du hasard)
score = (df["nombre_commandes"] / 30 + df["anciennete"] / 10
         - df["jours_depuis_derniere_commande"] / 365)
df["a_commande"] = (score + rng.normal(0, 0.3, n) > 0.4).astype(int)

# 3.4 : mêmes colonnes, dans le même ordre, que l'entrée du modèle
FEATURES = ["age", "anciennete", "nombre_commandes", "montant_total",
            "montant_moyen", "jours_depuis_derniere_commande"]
X = df[FEATURES]
y = df["a_commande"]

# apprentissage sur 80 % des données, vérification sur les 20 % restants
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)
print("Précision sur les données de test :", round(model.score(X_test, y_test), 2))

# 3.1 : sauvegarde du modèle
joblib.dump(model, "modele_commande.pkl")
print("Modèle sauvegardé dans modele_commande.pkl")