import os
import sqlite3
import pandas as pd
from datetime import datetime

DB_DIR = "database"
DB_PATH = os.path.join(DB_DIR, "crypto_data.db")

def procesar_datos():
    raw_data_path = 'data/raw/cryptos_data.csv'
    if not os.path.exists(raw_data_path):
        print("No se encontraron datos crudos para procesar.")
        return

    df_raw = pd.read_csv(raw_data_path)
    df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'], errors='coerce')

    # Crear directorios si no existen
    os.makedirs(DB_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Crear tablas con symbol como clave primaria
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            max_supply REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            price_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timestamp DATETIME,
            current_price REAL,
            market_cap REAL,
            total_volume REAL,
            price_change_24h REAL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (symbol) REFERENCES tokens(symbol),
            UNIQUE (symbol, timestamp)
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            metadata_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            ath REAL,
            ath_date DATETIME,
            atl REAL,
            atl_date DATETIME,
            roi_times REAL,
            roi_currency TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (symbol) REFERENCES tokens(symbol)
        )
        ''')

        # Insertar tokens
        tokens_df = df_raw[['symbol', 'name', 'max_supply']].drop_duplicates(subset=['symbol'])
        for _, row in tokens_df.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO tokens (symbol, name, max_supply)
                VALUES (?, ?, ?)
            ''', (row['symbol'], row['name'], row['max_supply']))

        # Crear los DataFrames para las tablas relacionadas
        prices_df = df_raw[['symbol', 'timestamp', 'current_price', 'market_cap', 'total_volume', 'price_change_24h']].copy()
        # Crear el DataFrame metadata_df asegurando que todas las columnas necesarias están presentes
        columns_required = ['symbol', 'ath', 'ath_date', 'atl', 'atl_date', 'roi_times', 'roi_currency']

        # Verificar columnas faltantes y agregarlas con valores None
        for col in columns_required:
            if col not in df_raw.columns:
                df_raw[col] = None

        metadata_df = df_raw[columns_required].copy()

        # Convertir columnas de tipo datetime a cadenas para evitar errores con SQLite
        prices_df['timestamp'] = prices_df['timestamp'].astype(str)
        metadata_df['ath_date'] = metadata_df['ath_date'].astype(str)
        metadata_df['atl_date'] = metadata_df['atl_date'].astype(str)

        # Insertar datos en la tabla prices
        for _, row in prices_df.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO prices (symbol, timestamp, current_price, market_cap, total_volume, price_change_24h)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (row['symbol'], row['timestamp'], row['current_price'], row['market_cap'], row['total_volume'], row['price_change_24h']))

        # Insertar datos en la tabla metadata
        for _, row in metadata_df.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO metadata (symbol, ath, ath_date, atl, atl_date, roi_times, roi_currency)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (row['symbol'], row['ath'], row['ath_date'], row['atl'], row['atl_date'], row['roi_times'], row['roi_currency']))

        conn.commit()
        print(f"Procesamiento completado y datos guardados en {DB_PATH}.")

    except sqlite3.Error as e:
        print(f"Error en la base de datos: {e}")
    finally:
        conn.close()

    # Registrar log
    log_dir = 'data/processed'
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, 'log.txt')
    with open(log_path, 'a') as log_file:
        log_file.write(f'Datos procesados el {datetime.now()}\n')

