"""Script mínimo para probar la funcionalidad básica"""
import sys
print("Iniciando script de prueba mínimo...")

try:
    # Intentar importar las dependencias principales
    print("Importando dependencias...")
    import pymongo
    print("✓ pymongo importado correctamente")
    import pandas as pd
    print("✓ pandas importado correctamente")
    import numpy as np
    print("✓ numpy importado correctamente")
    from sklearn.cluster import KMeans
    print("✓ sklearn importado correctamente")
    from dotenv import load_dotenv
    print("✓ dotenv importado correctamente")
    import certifi
    print("✓ certifi importado correctamente")
    
    # Cargar variables de entorno
    print("Cargando variables de entorno...")
    load_dotenv()
    import os
    mongo_uri = os.getenv('MONGO_URI', 'No configurado')
    db_name = os.getenv('DB_NAME', 'No configurado')
    print(f"DB_NAME: {db_name}")
    
    # Intentar conectar a MongoDB
    print("Intentando conexión a MongoDB...")
    client = pymongo.MongoClient(
        mongo_uri,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=5000
    )
    client.server_info()
    print("✓ Conexión a MongoDB exitosa")
    
    # Listar colecciones
    db = client[db_name]
    collections = db.list_collection_names()
    print(f"Colecciones disponibles: {collections}")
    
    # Verificar si existe la colección de ventas
    if 'ventas' in collections:
        count = db.ventas.count_documents({})
        print(f"Colección 'ventas' tiene {count} documentos")
        
    print("Script completado con éxito")

except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()

print("Script finalizado")
input("Presiona Enter para salir...")