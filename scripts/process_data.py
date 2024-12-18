import pandas as pd
import os
from datetime import datetime
import sqlite3

# Paths de los archivos
raw_path = 'data/raw/cryptos_data.csv'
processed_path = 'data/processed/cryptos_processed.csv'
db_path = 'data/cryptos_database.db'

# Cargar datos RAW
df_raw = pd.read_csv(raw_path)

# Procesar columnas innecesarias
df_raw.drop(columns=['image'], inplace=True)  # Eliminar columnas no necesarias
df_raw['last_updated'] = pd.to_datetime(df_raw['last_updated'], errors='coerce')  # Convertir last_updated a datetime

# Limpiar y transformar columnas
df_raw['atl_date'] = pd.to_datetime(df_raw['atl_date'], errors='coerce')  # Formatear atl_date
df_raw['ath_date'] = pd.to_datetime(df_raw['ath_date'], errors='coerce')  # Formatear ath_date

# Manejar columna ROI
if 'roi' in df_raw.columns:
    df_raw['roi_times'] = df_raw['roi'].apply(lambda x: eval(x)['times'] if isinstance(x, str) and 'times' in eval(x) else 0)
    df_raw['roi_currency'] = df_raw['roi'].apply(lambda x: eval(x)['currency'] if isinstance(x, str) and 'currency' in eval(x) else 'unknown')
    df_raw.drop(columns=['roi'], inplace=True)

# Convertir valores numéricos
numeric_columns = ['current_price', 'market_cap', 'total_volume', 'high_24h', 'low_24h', 'price_change_24h', 
                   'market_cap_change_24h', 'circulating_supply', 'total_supply', 'max_supply', 'ath', 'atl', 'roi_times']
for col in numeric_columns:
    if col in df_raw.columns:
        df_raw[col] = pd.to_numeric(df_raw[col], errors='coerce')

# Agregar marca temporal para mantener históricos
df_raw['data_fetched_at'] = datetime.now()  # Fecha de ejecución

# ---- Cargar datos procesados previos (si existen) ---- #
if os.path.exists(processed_path):
    # Intentar leer el archivo procesado existente
    try:
        df_processed = pd.read_csv(processed_path, parse_dates=['atl_date', 'ath_date', 'last_updated', 'data_fetched_at'])
        # Combinar datos nuevos con los antiguos
        df_combined = pd.concat([df_processed, df_raw], ignore_index=True)
        # Eliminar duplicados basados en el ID y la última actualización
        df_combined = df_combined.sort_values(by=['id', 'last_updated'], ascending=[True, False])
        df_combined = df_combined.drop_duplicates(subset=['id'], keep='first')
    except ValueError as e:
        print(f"Error al leer el archivo procesado previo: {e}")
        df_combined = df_raw
else:
    df_combined = df_raw

# Guardar datos procesados
df_combined.to_csv(processed_path, index=False)
print("Datos procesados guardados en archivo CSV.")

# ---- CREAR Y ACTUALIZAR BASE DE DATOS SQLITE ---- #
# Conexión a SQLite
conn = sqlite3.connect(db_path)

# Crear o actualizar la tabla en SQLite
df_combined.to_sql('cryptocurrencies', conn, if_exists='append', index=False)

# Confirmación
print(f"Base de datos SQLite actualizada en {db_path}")

# Cerrar conexión
conn.close()

with open('data/processed/log.txt', 'a') as log_file:
    log_file.write(f'Datos procesados el {datetime.now()}\n')
