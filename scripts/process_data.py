import pandas as pd

# Cargar el archivo CSV con los datos de las criptomonedas
df = pd.read_csv('data/raw/cryptos_data.csv')

# Limpiar y transformar los datos
df.drop(columns=['image', 'last_updated'], inplace=True)
df.fillna(0, inplace=True)
df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce')
df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')

# Guardar los datos procesados en un nuevo archivo CSV
df.to_csv('data/processed/cryptos_processed.csv', index=False)

# Imprimir los primeros datos procesados
print(df.head())
