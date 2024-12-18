import requests
import pandas as pd
from datetime import datetime

# Función para obtener datos de CoinGecko
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
        # Convertimos los datos a formato JSON
        data = response.json()
        
        # Creamos un DataFrame con los datos obtenidos
        df = pd.DataFrame(data)
        
        # Guardamos los datos en un archivo CSV
        df.to_csv('data/raw/cryptos_data.csv', index=False)
        
        print("Datos obtenidos y guardados con éxito.")
    else:
        print("Error al obtener datos de la API.")

with open('data/raw/log.txt', 'a') as log_file:
    log_file.write(f'Datos actualizados el {datetime.now()}\n')

# Ejecutamos la función
if __name__ == '__main__':
    obtener_datos_criptomonedas()
