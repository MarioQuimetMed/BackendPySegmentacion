from sklearn.cluster import KMeans
import numpy as np
import joblib
import os
from datetime import datetime

MODEL_PATH = "models/kmeans_model.pkl"

def model_needs_retraining():
    if not os.path.exists(MODEL_PATH): return True
    last = datetime.fromtimestamp(os.path.getmtime(MODEL_PATH))
    return (datetime.now() - last).days > 7

def get_or_train_kmeans(data):
    if not model_needs_retraining():
        print("✅ Usando modelo KMeans ya entrenado")
        return joblib.load(MODEL_PATH)

    print("🔁 Entrenando nuevo modelo KMeans")
    kmeans = KMeans(n_clusters=4, random_state=42)
    kmeans.fit(data)
    joblib.dump(kmeans, MODEL_PATH)
    return kmeans

def train_kmeans_model(df):
    model = get_or_train_kmeans(df[["Recencia", "Frecuencia", "Monetario"]])
    df["Segmento"] = model.predict(df[["Recencia", "Frecuencia", "Monetario"]])
    centroids = model.cluster_centers_
    orden = np.argsort(-centroids.sum(axis=1))
    nombres = {orden[0]: "VIP", orden[1]: "Fieles", orden[2]: "Ocasionales", orden[3]: "Dormidos"}
    df["Segmento_Nombre"] = df["Segmento"].map(nombres)
    return df