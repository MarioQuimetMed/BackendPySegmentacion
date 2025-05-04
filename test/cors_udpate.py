# Código para añadir a tu app.py existente:
from flask_cors import CORS

# Justo después de crear la app Flask:
# app = Flask(__name__)

# Añadir esta línea:
CORS(app, resources={r"/api/*": {"origins": os.getenv('ALLOW_ORIGINS', '*').split(',')}})