import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import logging

logger = logging.getLogger(__name__)

def process_rfm_data(rfm_data):
    """Procesar y escalar datos RFM"""
    try:
        # Convertir a DataFrame
        df_rfm = pd.DataFrame(rfm_data)
        
        # Verificar si hay datos suficientes
        if len(df_rfm) == 0:
            logger.warning("No hay datos RFM para procesar")
            return pd.DataFrame()
            
        # Verificar columnas necesarias
        required_cols = ["cliente_id", "recencia_dias", "num_compras", "total_gastado"]
        if not all(col in df_rfm.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df_rfm.columns]
            logger.error(f"Faltan columnas requeridas: {missing}")
            raise ValueError(f"Faltan columnas requeridas: {missing}")
        
        # Renombrar columnas
        df_rfm.rename(columns={
            "recencia_dias": "Recencia",
            "num_compras": "Frecuencia",
            "total_gastado": "Monetario"
        }, inplace=True)
        
        # Convertir a tipo numérico y manejar posibles errores
        for col in ["Recencia", "Frecuencia", "Monetario"]:
            df_rfm[col] = pd.to_numeric(df_rfm[col], errors='coerce')
            
        # Eliminar nulos y duplicados
        before_clean = len(df_rfm)
        df_rfm = df_rfm.dropna(subset=["Recencia", "Frecuencia", "Monetario"]).drop_duplicates()
        after_clean = len(df_rfm)
        
        if before_clean > after_clean:
            logger.warning(f"Se eliminaron {before_clean - after_clean} filas con valores nulos o duplicados")
        
        # Inversión de Recencia (menor recencia = mejor)
        max_recencia = df_rfm["Recencia"].max()
        df_rfm["Recencia"] = max_recencia - df_rfm["Recencia"]
        
        # Seleccionar métricas RFM
        rfm_metrics = df_rfm[["Recencia", "Frecuencia", "Monetario"]]
        
        # Escalar datos
        scaler = StandardScaler()
        rfm_scaled = scaler.fit_transform(rfm_metrics)
        
        # Crear DataFrame con datos escalados
        df_rfm_scaled = pd.DataFrame(rfm_scaled, columns=["Recencia", "Frecuencia", "Monetario"])
        df_rfm_scaled["cliente_id"] = df_rfm["cliente_id"].values
        
        logger.info("Datos RFM procesados y escalados exitosamente")
        return df_rfm_scaled
    except Exception as e:
        logger.error(f"Error procesando datos RFM: {str(e)}")
        raise