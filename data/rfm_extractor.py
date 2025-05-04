from db.mongo import get_db
from datetime import datetime

def extract_rfm_data():
    db = get_db()
    fecha_actual = datetime.now()
    pipeline = [
        {"$match": {"estado": {"$in": ["Procesado", "Completado", "Entregado"]}}},
        {"$group": {
            "_id": "$cliente",
            "ultima_compra": {"$max": "$createdAT"},
            "num_compras": {"$sum": 1},
            "total_gastado": {"$sum": "$total"}
        }},
        {"$project": {
            "cliente_id": "$_id",
            "recencia_dias": {"$dateDiff": {"startDate": "$ultima_compra", "endDate": fecha_actual, "unit": "day"}},
            "num_compras": 1,
            "total_gastado": 1
        }}
    ]
    return list(db.ventas.aggregate(pipeline))