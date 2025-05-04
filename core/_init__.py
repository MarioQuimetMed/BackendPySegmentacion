from .rfm_analysis import extract_rfm_data, save_results_to_db, get_customer_segment
from .data_processing import process_rfm_data
from .segmentation import train_kmeans_model

def run_rfm_analysis():
    """Proceso completo de análisis RFM"""
    # Extraer datos RFM de MongoDB
    rfm_data = extract_rfm_data()
    
    # Procesar y escalar datos
    df_rfm_scaled = process_rfm_data(rfm_data)
    
    # Entrenar modelo KMeans
    df_rfm_segments = train_kmeans_model(df_rfm_scaled)
    
    # Guardar resultados en MongoDB
    records_saved = save_results_to_db(df_rfm_segments)
    
    # Devolver estadísticas
    segment_counts = df_rfm_segments["Segmento_Nombre"].value_counts().to_dict() if not df_rfm_segments.empty else {}
    
    return {
        "success": True,
        "segments_count": segment_counts,
        "records_processed": len(rfm_data),
        "records_saved": records_saved
    }