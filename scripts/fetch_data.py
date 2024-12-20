import requests
import pandas as pd
from datetime import datetime
import os

def obtener_datos_criptomonedas():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 100,
        'page': 1
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        # Convertir a JSON y DataFrame
        data = response.json()
        df = pd.DataFrame(data)

        # Agregar timestamp
        df['timestamp'] = datetime.now()

        # Crear directorio si no existe
        os.makedirs('data/raw', exist_ok=True)

        # Guardar CSV
        df.to_csv('data/raw/cryptos_data.csv', index=False)
        print("Datos obtenidos y guardados con éxito en data/raw/cryptos_data.csv.")
    else:
        print(f"Error al obtener datos de la API. Código de estado: {response.status_code}")

with open('data/raw/log.txt', 'a') as log_file:
    log_file.write(f'Datos actualizados el {datetime.now()}\n')
