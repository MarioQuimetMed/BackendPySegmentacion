"""Verificar todas las dependencias necesarias"""
import sys
import importlib.util
import subprocess

# Lista de dependencias requeridas
dependencies = [
    "flask",
    "pymongo",
    "pandas",
    "numpy",
    "scikit-learn",
    "python-dotenv",
    "certifi",
    "matplotlib"
]

print("Verificando dependencias instaladas...")

missing_deps = []
for dep in dependencies:
    try:
        spec = importlib.util.find_spec(dep)
        if spec is None:
            print(f"❌ {dep}: NO INSTALADO")
            missing_deps.append(dep)
        else:
            module = importlib.import_module(dep)
            version = getattr(module, '__version__', 'desconocida')
            print(f"✓ {dep}: Instalado (versión {version})")
    except ImportError:
        print(f"❌ {dep}: NO INSTALADO")
        missing_deps.append(dep)

if missing_deps:
    print("\nDependencias faltantes. Intentando instalar...")
    for dep in missing_deps:
        print(f"Instalando {dep}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
            print(f"✓ {dep} instalado correctamente")
        except subprocess.CalledProcessError:
            print(f"❌ Error al instalar {dep}")

print("\nVerificación completada.")
input("Presiona Enter para continuar...")