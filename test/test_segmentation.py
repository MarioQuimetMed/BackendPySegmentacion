from core import run_rfm_analysis
from db.connection import get_db
import json
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Script de prueba para el análisis RFM"""
    try:
        logger.info("Iniciando prueba de segmentación RFM")
        
        # Ejecutar el análisis
        result = run_rfm_analysis()
        logger.info(f"Resultado de segmentación: {json.dumps(result, indent=2)}")
        
        # Obtener algunos clientes de ejemplo
        db = get_db()
        sample_customers = list(db.customer_segments.find().limit(5))
        
        # Mostrar ejemplos
        logger.info("Ejemplos de segmentos de clientes:")
        for i, customer in enumerate(sample_customers):
            # Convertir ObjectId a string para impresión
            customer['_id'] = str(customer['_id'])
            logger.info(f"Cliente {i+1}: {json.dumps(customer, indent=2, default=str)}")
        
        logger.info("Prueba de segmentación RFM completada")
    except Exception as e:
        logger.error(f"Error en la prueba de segmentación: {str(e)}")

if __name__ == "__main__":
    main()