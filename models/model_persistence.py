from db.mongo import get_db
from datetime import datetime
import pytz
from bson.objectid import ObjectId

def save_results_to_db(df):
    db = get_db()

    # 1. Obtener fecha actual en zona horaria de Bolivia
    bolivia_timezone = pytz.timezone("America/La_Paz")
    now_bolivia = datetime.now(bolivia_timezone)

    # 2. Crear un nuevo version_id único basado en ObjectId
    version_id = str(ObjectId())

    # 3. Preparar los nuevos registros
    records = []
    for _, row in df.iterrows():
        records.append({
            "cliente_id": row["cliente_id"],
            "recencia_dias": float(row["Recencia"]),
            "num_compras": float(row["Frecuencia"]),
            "total_gastado": float(row["Monetario"]),
            "segmento": row["Segmento_Nombre"],
            "segmento_numero": int(row["Segmento"]),
            "fecha_calculo": now_bolivia,
            "version_id": version_id  # ✅ Versión del análisis
        })

    # 4. Insertar los nuevos registros sin borrar los anteriores
    db.customer_segments.insert_many(records)
