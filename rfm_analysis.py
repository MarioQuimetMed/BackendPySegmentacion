from data.rfm_extractor import extract_rfm_data
from preprocessing.rfm_preprocessor import process_rfm_data
from clustering.rfm_cluster import train_kmeans_model
from models.model_persistence import save_results_to_db
from db.mongo import get_db
from bson import ObjectId
from datetime import datetime
import pytz
import os
import sys


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def run_segmentation(force=False):
    """
    Ejecuta la segmentación RFM si hay suficientes nuevos datos o si force=True.
    """
    print("=== INICIANDO ANÁLISIS RFM ===")
    db = get_db()

    # Obtener última fecha de segmentación
    last_seg = db.customer_segments.find_one(sort=[("fecha_calculo", -1)])
    last_seg_date = last_seg["fecha_calculo"] if last_seg else None

    # Hora actual Bolivia
    bolivia_tz = pytz.timezone('America/La_Paz')
    now_bolivia = datetime.now(bolivia_tz)

    # Verificar si hay suficientes nuevos datos
    if not force and last_seg_date:
        nuevas_ventas = db.ventas.count_documents({
            "createdAT": {"$gt": last_seg_date},
            "estado": {"$in": ["Procesado", "Completado", "Entregado"]}
        })
        if nuevas_ventas < 50:
            print(f"🚫 No hay suficientes ventas nuevas ({nuevas_ventas}) para recalcular.")
            return {
                "success": False,
                "message": f"No hay suficientes datos nuevos ({nuevas_ventas} ventas nuevas).",
                "new_data": nuevas_ventas
            }

    # Extraer y procesar datos RFM
    rfm_data = extract_rfm_data()
    df_rfm_scaled = process_rfm_data(rfm_data)
    df_rfm_segments = train_kmeans_model(df_rfm_scaled)

    # Guardar nueva segmentación
    save_results_to_db(df_rfm_segments)

    return {
        "success": True,
        "segments": df_rfm_segments["Segmento_Nombre"].value_counts().to_dict(),
        "records_processed": len(rfm_data),
        "records_saved": len(df_rfm_segments),
        "timestamp": now_bolivia.isoformat()
    }

def get_customer_segment(customer_id):
    """
    Obtiene el segmento de un cliente específico.
    """
    db = get_db()

    # Buscar por cliente_id (string o ObjectId)
    segment = db.customer_segments.find_one({"cliente_id": customer_id}) or \
              (ObjectId.is_valid(customer_id) and db.customer_segments.find_one({"cliente_id": ObjectId(customer_id)}))

    if segment and "_id" in segment:
        segment["_id"] = str(segment["_id"])

    return segment
