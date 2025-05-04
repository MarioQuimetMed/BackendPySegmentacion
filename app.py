from flask import Flask, jsonify, request
from flask_cors import CORS
import sys
import os
import logging
from datetime import datetime
import pytz
from bson.objectid import ObjectId

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from rfm_analysis import run_segmentation, get_customer_segment
from db.mongo import get_db

# --- Configuración general ---
load_dotenv()

PORT = int(os.environ.get("PORT", 5000))
DEBUG = os.environ.get("DEBUG", "False") == "True"

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger('app')

# --- Crear app Flask ---
app = Flask(__name__)
CORS(app)

# --- Endpoints ---

@app.route("/")
def home():
    return """
    <html>
    <head><title>Segmentación RFM</title></head>
    <body>
        <h1>API de Segmentación RFM</h1>
        <ul>
            <li><a href="/api/health">Verificar estado</a></li>
        </ul>
    </body>
    </html>
    """

@app.route("/api/health")
def health_check():
    tz = pytz.timezone("America/La_Paz")
    return jsonify({
        "status": "ok",
        "service": "rfm-segmentation",
        "timestamp": datetime.now(tz).isoformat()
    })

@app.route("/api/segmentation/run", methods=["POST"])
def trigger_segmentation():
    try:
        force = request.args.get('force', 'false').lower() == 'true'
        logger.info("Ejecutando segmentación desde API")
        result = run_segmentation(force=force)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error al ejecutar segmentación: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/customer/segment/<customer_id>", methods=["GET"])
def api_get_customer_segment(customer_id):
    try:
        segment = get_customer_segment(customer_id)
        if segment:
            return jsonify({"success": True, "data": segment})
        else:
            return jsonify({"success": False, "message": "Cliente no encontrado"}), 404
    except Exception as e:
        logger.error(f"Error obteniendo segmento: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/segmentation/customers', methods=['GET'])
def get_all_customer_segments():
    """
    Devuelve datos RFM y segmento por cliente SOLO de la última versión.
    """
    try:
        db = get_db()

        # Obtener el último version_id
        last_segment = db.customer_segments.find_one(sort=[("fecha_calculo", -1)])
        if not last_segment:
            return jsonify({"success": False, "message": "No hay datos de segmentación"}), 404

        version_id = last_segment.get("version_id")
        if not version_id:
            return jsonify({"success": False, "message": "No se encontró version_id en los datos"}), 404

        # Traer clientes SOLO de esa versión
        resultados = list(db.customer_segments.find({"version_id": version_id}))
        clientes = [{
            "cliente_id": str(r.get("cliente_id")),
            "recencia_dias": r.get("recencia_dias"),
            "num_compras": r.get("num_compras"),
            "total_gastado": r.get("total_gastado"),
            "segmento": r.get("segmento")
        } for r in resultados]

        return jsonify({"success": True, "clientes": clientes})
    except Exception as e:
        logger.error(f"Error extrayendo datos de clientes: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/segmentation/status", methods=["GET"])
def get_segmentation_status():
    try:
        db = get_db()

        last_segment = db.customer_segments.find_one(sort=[("fecha_calculo", -1)])
        if not last_segment:
            return jsonify({
                "success": False,
                "message": "No hay segmentaciones realizadas"
            }), 404

        pipeline = [
            {"$match": {"version_id": last_segment["version_id"]}},
            {"$group": {"_id": "$segmento", "count": {"$sum": 1}}}
        ]
        segment_counts = {}
        for doc in db.customer_segments.aggregate(pipeline):
            segment_counts[doc["_id"]] = doc["count"]

        # ⚡ Corrección aquí: forzar fecha en zona horaria Bolivia
        from datetime import datetime
        import pytz
        bolivia_tz = pytz.timezone('America/La_Paz')
        fecha_bolivia = last_segment["fecha_calculo"].astimezone(bolivia_tz).isoformat()

        return jsonify({
            "success": True,
            "last_update": fecha_bolivia,
            "segments": segment_counts,
            "total_customers": sum(segment_counts.values())
        })
    except Exception as e:
        logger.error(f"Error obteniendo estado de segmentación: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/segmentation/check-new-data", methods=["GET"])
def check_new_data():
    try:
        db = get_db()

        last_seg = db.customer_segments.find_one(sort=[("fecha_calculo", -1)])
        if not last_seg:
            return jsonify({"new_data_count": "unknown", "should_train": True})

        last_date = last_seg["fecha_calculo"]

        # Contar ventas nuevas
        count = db.ventas.count_documents({
            "createdAT": {"$gt": last_date},
            "estado": {"$in": ["Procesado", "Completado", "Entregado"]}
        })

        return jsonify({
            "success": True,
            "new_data_count": count,
            "should_train": count > 50
        })
    except Exception as e:
        logger.error(f"Error chequeando nuevos datos: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/clientes", methods=["GET"])
def get_clientes_fullname():
    """
    Devuelve todos los clientes con su cliente_id y fullname.
    """
    try:
        db = get_db()

        # Buscar todos los clientes
        resultados = db.clientes.find()

        clientes = []
        for cliente in resultados:
            clientes.append({
                "cliente_id": str(cliente["_id"]),
                "fullname": cliente.get("fullname", "Nombre Desconocido")
            })

        return jsonify({"success": True, "clientes": clientes})

    except Exception as e:
        logger.error(f"Error obteniendo clientes: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

# Nueva ruta para obtener la información solicitada

@app.route("/api/clientes/detalles", methods=["GET"])
def get_clientes_info():
    """
    Versión optimizada que usa agregaciones de MongoDB para calcular los datos
    requeridos directamente en la base de datos sin hacer múltiples consultas.
    """
    try:
        db = get_db()
        
        # Definir la página y los límites de la consulta
        page = int(request.args.get('page', 1))  # Página por defecto = 1
        limit = int(request.args.get('limit', 100))  # Límite reducido para evitar timeouts
        
        # Obtener IDs de clientes para esta página
        cliente_ids = []
        for cliente in db.clientes.find({}, {"_id": 1}).skip((page - 1) * limit).limit(limit):
            cliente_ids.append(cliente["_id"])
        
        logger.info(f"Procesando {len(cliente_ids)} clientes para la página {page}")
        
        # Usar agregación para calcular estadísticas de ventas para estos clientes
        pipeline = [
            # Filtrar solo ventas de los clientes en esta página
            {"$match": {"cliente": {"$in": cliente_ids}}},
            
            # Agrupar por cliente y calcular métricas
            {"$group": {
                "_id": "$cliente",
                "cantidad_de_compras": {"$sum": 1},
                "costo_de_compras": {"$sum": "$total"},
                "ultima_compra": {"$max": "$createdAT"}
            }}
        ]
        
        # Ejecutar la agregación
        ventas_stats = {str(stats["_id"]): stats for stats in db.ventas.aggregate(pipeline)}
        
        # Preparar resultado final
        clientes_info = []
        
        for cliente_id_obj in cliente_ids:
            cliente_id_str = str(cliente_id_obj)
            stats = ventas_stats.get(cliente_id_str, {})
            
            # Formatear fecha de última compra si existe
            ultima_compra = stats.get("ultima_compra")
            ultima_compra_formateada = ultima_compra.strftime("%d/%m/%Y") if ultima_compra else None
            
            clientes_info.append({
                "cliente_id": cliente_id_str,
                "cantidad_de_compras": stats.get("cantidad_de_compras", 0),
                "costo_de_compras": stats.get("costo_de_compras", 0),
                "ultima_compra": ultima_compra_formateada
            })
        
        logger.info(f"Completado procesamiento para {len(clientes_info)} clientes")
        return jsonify({"success": True, "clientes_info": clientes_info})

    except Exception as e:
        logger.error(f"Error obteniendo información de clientes: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500
# --- Run App ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
