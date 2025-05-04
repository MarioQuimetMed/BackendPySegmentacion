from datetime import datetime
import logging
import pandas as pd
from bson import ObjectId
from db.connection import get_db

logger = logging.getLogger(__name__)

def extract_rfm_data():
    """Extraer datos RFM desde MongoDB"""
    db = get_db()
    
    # Fecha actual para cálculo de recencia
    fecha_actual = datetime.now()
    logger.info(f"Fecha de referencia para recencia: {fecha_actual}")
    
    pipeline = [
        # Filtrar ventas completadas
        {"$match": {"estado": {"$in": ["Procesado", "Completado", "Entregado"]}}},
        
        # Agrupar por cliente
        {"$group": {
            "_id": "$cliente",
            "ultima_compra": {"$max": "$createdAT"},
            "num_compras": {"$sum": 1},
            "total_gastado": {"$sum": "$total"}
        }},
        
        # Calcular recencia en días
        {"$project": {
            "cliente_id": "$_id",
            "recencia_dias": {
                "$dateDiff": {
                    "startDate": "$ultima_compra",
                    "endDate": fecha_actual,
                    "unit": "day"
                }
            },
            "num_compras": 1,
            "total_gastado": 1
        }}
    ]
    
    try:
        rfm_data = list(db.ventas.aggregate(pipeline, allowDiskUse=True))
        logger.info(f"Datos RFM extraídos: {len(rfm_data)} registros")
        
        # Convertir ObjectId a string para facilitar procesamiento
        for item in rfm_data:
            if isinstance(item["cliente_id"], ObjectId):
                item["cliente_id"] = str(item["cliente_id"])
                
        return rfm_data
    except Exception as e:
        logger.error(f"Error ejecutando pipeline de agregación: {str(e)}")
        raise

def save_results_to_db(df_rfm_segments):
    """Guardar resultados de segmentación en MongoDB"""
    db = get_db()
    
    # Transformar datos para guardar
    rfm_transformed = []
    for _, row in df_rfm_segments.iterrows():
        rfm_transformed.append({
            "cliente_id": row["cliente_id"],
            "recencia_dias": float(row["Recencia"]) if isinstance(row["Recencia"], (int, float)) else 0,
            "num_compras": float(row["Frecuencia"]) if isinstance(row["Frecuencia"], (int, float)) else 0,
            "total_gastado": float(row["Monetario"]) if isinstance(row["Monetario"], (int, float)) else 0,
            "segmento": row["Segmento_Nombre"],
            "segmento_numero": int(row["Segmento"]) if isinstance(row["Segmento"], (int, float)) else 0,
            "fecha_calculo": datetime.now()
        })
    
    try:
        # Crear índice si no existe
        db.customer_segments.create_index("cliente_id", unique=True)
        
        # Borrar resultados anteriores
        if rfm_transformed:
            db.customer_segments.delete_many({})
            
            # Insertar nuevos resultados
            db.customer_segments.insert_many(rfm_transformed)
        
        logger.info(f"Guardados {len(rfm_transformed)} segmentos de clientes en la base de datos")
        return len(rfm_transformed)
    except Exception as e:
        logger.error(f"Error guardando resultados en la base de datos: {str(e)}")
        raise

def get_customer_segment(customer_id):
    """Obtener segmento para un cliente específico"""
    db = get_db()
    
    try:
        # Intentar con string
        result = db.customer_segments.find_one({"cliente_id": customer_id})
        if not result and ObjectId.is_valid(customer_id):
            # Intentar con ObjectId
            result = db.customer_segments.find_one({"cliente_id": ObjectId(customer_id)})
            
        if result:
            # Convertir ObjectId a string para serialización JSON
            if "_id" in result:
                result["_id"] = str(result["_id"])
            return result
        return None
    except Exception as e:
        logger.error(f"Error obteniendo segmento de cliente: {str(e)}")
        raise