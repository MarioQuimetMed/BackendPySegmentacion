import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar variables desde .env si existe
load_dotenv()

# --- Configuración (puedes pegar aquí o usar .env) ---
LOCAL_URI = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/EcommerTenants")
REMOTE_URI = os.getenv("REMOTE_MONGO_URI", "mongodb+srv://houwenvt:will@cluster0.crz8eun.mongodb.net/EcommerTenants")

# --- Conexión a Mongo ---
local_client = MongoClient(LOCAL_URI)
remote_client = MongoClient(REMOTE_URI)

local_db = local_client.get_default_database()
remote_db = remote_client.get_default_database()

def migrar_datos():
    print(f"🗂 Conectado a local: {local_db.name}")
    print(f"🗂 Conectado a remoto: {remote_db.name}")

    # --- 1. Borrar todas las colecciones remotas ---
    print("🚮 Borrando colecciones en la base de datos remota...")
    for nombre in remote_db.list_collection_names():
        remote_db.drop_collection(nombre)
        print(f"   ✔ Eliminada: {nombre}")

    # --- 2. Migrar colecciones una por una ---
    print("📤 Migrando datos de local a remoto...")
    for nombre in local_db.list_collection_names():
        local_coll = local_db[nombre]
        remote_coll = remote_db[nombre]

        documentos = list(local_coll.find())
        if documentos:
            remote_coll.insert_many(documentos)
            print(f"   ✔ {nombre}: {len(documentos)} documentos migrados.")
        else:
            print(f"   ⚠ {nombre} está vacía.")

    print("✅ Migración completa.")

if __name__ == "__main__":
    try:
        migrar_datos()
    except Exception as e:
        print(f"❌ Error durante la migración: {e}")
