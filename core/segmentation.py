import os
import logging
import numpy as np
from sklearn.cluster import KMeans
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Parámetros de segmentación
NUM_CLUSTERS = int(os.getenv('NUM_CLUSTERS', 4))
SEGMENT_NAMES = {
    0: "Dormidos",
    1: "Fieles",
    2: "VIP", 
    3: "Ocasionales"
}

def train_kmeans_model(df_rfm_scaled):
    """Entrenar modelo KMeans en datos RFM"""
    try:
        if len(df_rfm_scaled) == 0:
            logger.warning("No hay datos suficientes para entrenar el modelo")
            return df_rfm_scaled
            
        # Seleccionar características para clustering
        rfm_features = df_rfm_scaled[["Recencia", "Frecuencia", "Monetario"]]
        
        # Entrenar modelo KMeans
        kmeans = KMeans(n_clusters=NUM_CLUSTERS, random_state=42, n_init=10)
        df_rfm_scaled["Segmento"] = kmeans.fit_predict(rfm_features)
        
        # Calcular centroides para interpretación
        centroids = kmeans.cluster_centers_
        
        # Analizar centroides para asignar nombres adecuadamente
        # Mayor valor = mejor en todas las dimensiones
        segment_interpretation = {}
        
        # Asignar nombres según características de los centroides
        centroid_sums = centroids.sum(axis=1)
        centroid_ranks = np.argsort(-centroid_sums)  # Ordenar de mayor a menor valor
        
        # VIP: mayor valor combinado de RFM
        segment_interpretation[centroid_ranks[0]] = "VIP"
        
        # Dormidos: menor valor combinado de RFM
        segment_interpretation[centroid_ranks[-1]] = "Dormidos"
        
        # Fieles: segundo mejor valor combinado
        segment_interpretation[centroid_ranks[1]] = "Fieles"
        
        # Ocasionales: tercer mejor valor combinado
        segment_interpretation[centroid_ranks[2]] = "Ocasionales"
        
        # Añadir nombres de segmentos
        df_rfm_scaled["Segmento_Nombre"] = df_rfm_scaled["Segmento"].map(segment_interpretation)
        
        logger.info(f"Modelo KMeans entrenado con {NUM_CLUSTERS} clusters")
        logger.info(f"Interpretación de segmentos: {segment_interpretation}")
        
        # Contar clientes por segmento
        segment_counts = df_rfm_scaled["Segmento_Nombre"].value_counts().to_dict()
        logger.info(f"Distribución de clientes por segmento: {segment_counts}")
        
        return df_rfm_scaled
    except Exception as e:
        logger.error(f"Error entrenando modelo KMeans: {str(e)}")
        raise