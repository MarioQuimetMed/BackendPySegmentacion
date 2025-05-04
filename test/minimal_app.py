"""Versión mínima de app.py para detectar errores"""
import sys
print("Iniciando minimal_app.py...")

try:
    from flask import Flask, jsonify
    import os
    from dotenv import load_dotenv
    
    # Cargar variables de entorno
    print("Cargando configuración...")
    load_dotenv()
    
    # Crear aplicación Flask
    print("Creando aplicación Flask...")
    app = Flask(__name__)
    
    @app.route('/')
    def home():
        return "Servidor RFM funcionando"
    
    @app.route('/api/health')
    def health():
        return jsonify({"status": "ok"})
    
    # Iniciar servidor
    if __name__ == '__main__':
        PORT = int(os.getenv('PORT', 5000))
        print(f"Iniciando servidor en puerto {PORT}...")
        print(f"Visita http://localhost:{PORT}/api/health")
        app.run(host='0.0.0.0', port=PORT, debug=True)

except Exception as e:
    print(f"ERROR EN APP: {str(e)}")
    import traceback
    traceback.print_exc()

print("Script app.py finalizado")