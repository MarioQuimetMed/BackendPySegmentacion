import pandas as pd
from sklearn.preprocessing import StandardScaler

def process_rfm_data(data):
    df = pd.DataFrame(data)
    df.rename(columns={"recencia_dias": "Recencia", "num_compras": "Frecuencia", "total_gastado": "Monetario"}, inplace=True)
    df["Recencia"] = df["Recencia"].max() - df["Recencia"]
    df.dropna(inplace=True)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(df[["Recencia", "Frecuencia", "Monetario"]])
    result = pd.DataFrame(scaled, columns=["Recencia", "Frecuencia", "Monetario"])
    result["cliente_id"] = df["cliente_id"].values
    return result