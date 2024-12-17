# Crypto Analytics

**Crypto Analytics** es un proyecto que obtiene y procesa datos de criptomonedas utilizando la API de CoinGecko. Este proyecto tiene como objetivo analizar y visualizar los datos de las criptomonedas, permitiendo crear gráficos interactivos para observar su rendimiento en el mercado.

## Estructura del Proyecto

El proyecto está organizado de la siguiente manera:

my-project/
├── data/ \n
│   ├── raw/                  # Archivos de datos originales \n
│   │   └── cryptos_data.csv  # CSV con los datos obtenidos de la API \n
│   └── processed/            # Archivos de datos procesados \n
├── notebooks/                # Notebooks de análisis
├── scripts/                  # Carpeta para scripts de procesamiento y análisis de datos
│   ├── fetch_data.py         # Obtiene los datos de la API
│   └── process_data.py       # Procesa los datos obtenidos
├── requirements.txt          # Dependencias del proyecto
├── README.md                 # Documentación del proyecto
└── .gitignore                # Archivos que deben ser ignorados por Git

## Descripción de los Scripts

### `scripts/fetch_data.py`
Este script se encarga de obtener los datos de la API de CoinGecko y guardarlos en un archivo CSV en la carpeta `data/raw/`. Utiliza la librería `requests` para hacer las solicitudes y `pandas` para almacenar los datos en formato CSV.

### `scripts/process_data.py`
Este script carga los datos desde el archivo `cryptos_data.csv`, realiza un proceso de limpieza y transformación, y guarda los datos procesados en un nuevo archivo CSV en `data/processed/`.

## Requisitos

Tener las siguientes librerías instaladas:

- `requests` para hacer las solicitudes HTTP.
- `pandas` para el procesamiento de datos.

Puedes instalar las dependencias con el siguiente comando:

```bash
pip install -r requirements.txt
