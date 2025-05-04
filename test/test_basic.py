import os
import pymongo
import certifi
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

print("=== PRUEBA BÁSICA DE CONEXIÓN ===")

# Mostrar variables de entorno (sin contraseñas)
mongo_uri = os.getenv('MONGO_URI', 'No configurado')
db_name = os.getenv('DB_NAME', 'No configurado')

# Solo mostrar parte de la URI por seguridad
if mongo_uri != 'No configurado':
    parts = mongo_uri.split('@')
    if len(parts) > 1:
        safe_uri = f"...@{parts[1]}"
    else:
        safe_uri = "URI malformada"
else:
    safe_uri = "No configurado"

print(f"MONGO_URI: {safe_uri}")
print(f"DB_NAME: {db_name}")

# Intentar conexión directa
try:
    print("Intentando conexión a MongoDB...")
    client = pymongo.MongoClient(
        mongo_uri,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=5000
    )
    
    # Verificar conexión
    client.server_info()
    print("✅ Conexión exitosa a MongoDB!")
    
    # Listar bases de datos
    dbs = client.list_database_names()
    print(f"Bases de datos disponibles: {dbs}")
    
    # Acceder a la base de datos configurada
    db = client[db_name]
    
    # Listar colecciones
    collections = db.list_collection_names()
    print(f"Colecciones en {db_name}: {collections}")
    
    # Contar documentos en algunas colecciones
    for col in collections[:3]:  # Mostrar solo las 3 primeras
        count = db[col].count_documents({})
        print(f"Colección '{col}': {count} documentos")
        
except Exception as e:
    print(f"❌ Error de conexión: {str(e)}")
    
print("=== FIN DE PRUEBA ===")