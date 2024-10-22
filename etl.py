from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
import utils
import pandas as pd


# Instancio un objeto ConfigParser que se encargará de leer el archivo .conf
parser = ConfigParser()
parser.read("pipeline.conf")

# Busco la sección que tenga las credenciales necesarias para la acceder a la API
api_credentials = parser["stockdata_api"]
api_token = api_credentials["api_token"]



'''
StockData API 
'''

# Definicion de función de extracción

def extraction(symbols_list, endpoint, base_url, params):

    data = []
    for symbol in symbols_list:
        if endpoint == 'entity/search':
            cust_endpoint = f"{endpoint}?search={symbol}"

        elif endpoint == 'data/intraday':
            cust_endpoint = f"{endpoint}?symbols={symbol}"

        json_data = utils.get_data(base_url, cust_endpoint, params)
        if json_data:
            data.extend(json_data)

    if data:
        df = utils.build_table(data)

    return df


'''
EXTRACCIÓN FULL

Se consulta el endpoint 'search' de búsqueda de entidades para determinadas empresas.
Se realiza una extraccion full ya que se extraen todos los datos de las mismas en cada ejecucion. 
'''

base_url = "https://api.stockdata.org/v1"
endpoint = "entity/search"
params = {
    "api_token": api_token
}
symbols = ['TSLA','AMD','AMZN','NVDA','V','GOOGL','MA','BRKB','MSFT','AAPL'] 

df_search = extraction(symbols, endpoint, base_url, params=params)
print(df_search)

# OBS: Dado que este endpoint devuelve entidades, además de los simbolos indicados, devuelve otros que matchean con caracteres del mismo.


'''
EXTRACCIÓN INCREMENTAL

Se consulta el endpoint 'intraday' de datos intradiarios de las acciones de dichas empresas.
Se realiza una extracción incremental ya que solo se obtendrán los datos del ultimo dia, adquiriendo cotizaciones por
minuto de cada acción.
La eleccion de este tipo de extraccion se fundamenta en la idea de que son datos dinamicos que se actualizan, por lo tanto
se buscará obtener los nuevos registros, si los hubiera.

'''

utc_date = datetime.now(timezone.utc)
end_date = utc_date.strftime("%Y-%m-%d")
start_date = (utc_date - timedelta(days=3)).strftime("%Y-%m-%d")
endpoint = "data/intraday"
params = {
    "date_from": start_date,
    "date_to": end_date,
    "api_token": api_token
}


df_intraday = extraction(symbols, endpoint, base_url, params=params)
print(df_intraday)



# Almacenamiento en la capa bronze del datalake.

bronze_dir = "datalake/bronze/stockdata_api"

utils.save_to_parquet(
    df_search,
    f"{bronze_dir}/stocks/data.parquet"
)


df_intraday["date"] = pd.to_datetime(df_intraday.date)
df_intraday["only_date"] = df_intraday.date.dt.date
df_intraday["hour"] = df_intraday.date.dt.hour

utils.save_to_parquet(
    df_intraday,
    f"{bronze_dir}/intraday_values",
    ["only_date", "hour"]
)



# --- Procesamiento para la capa silver ---

# Procesamiento df_intraday

# Leo los datos de la capa bronze
dir = f"{bronze_dir}/intraday_values"
df_intraday_bronze = utils.read_from_parquet(dir)

# Obtengo informacion de columnas, cantidad de registros nulos y tipo de datos.
 
df_intraday_info = df_intraday_bronze.info(memory_usage='deep')
has_null = df_intraday_bronze.isnull().any().any()

print(df_intraday_info)
print(has_null)

# El dataframe de intraday no contiene nulos, por lo que procederé a la conversión de tipos.

conversion_mapping_intraday = {
    "symbol": "string",
    "open_value": "float16",
    "high_value": "float16",
    "low_value": "float16",
    "close_value": "float16",
    "only_date": "datetime64[ns]",
    "hour": "datetime64[ns]"
}

# Renombre de columnas.

rename_mapping_intraday = {
    "ticker": "symbol",
    "data.open": "open_value",
    "data.high": "high_value",
    "data.low": "low_value",
    "data.close": "close_value",
    "data.volume": "trading_volume",
    "data.is_extended_hours": "is_extended_hours"
}


df_intraday_silver = df_intraday_bronze.rename(columns=rename_mapping_intraday)
df_intraday_silver = df_intraday_silver.astype(conversion_mapping_intraday)

print(df_intraday_silver.info(memory_usage='deep'))



# Procesamiento df_search

# Leo los datos de la capa bronze.
dir = f"{bronze_dir}/stocks"
df_search_bronze = utils.read_from_parquet(dir)

# Obtengo la cantidad de valores nulos por columna.
df_search_info = df_search_bronze.info(memory_usage='deep')
null_counts = df_search_bronze.isnull().sum()

print(df_search_info)
print(null_counts)

# Eliminacion de columnas con todos sus registros null. Estas columnas no aportan informacion y tampoco considero util definirles
# un valor por defecto.

df_search_bronze = df_search_bronze.dropna(axis=1, how='all') 

# Casteo de columnas

conversion_mapping_search = {
    "symbol": "string",
    "name": "string",
    "type": "category",
    "country": "string"
}

# Renombre de columna

rename_mapping_search = {
    "type": "stock_type"
}

df_search_bronze = df_search_bronze.astype(conversion_mapping_search)
df_search_bronze = df_search_bronze.rename(columns=rename_mapping_search)
print(df_search_bronze.info(memory_usage='deep'))



# Almacenamiento de dataframes preprocesados en capa silver

silver_dir = "datalake/silver/stockdata_api"

utils.save_to_parquet(
    df_search_bronze,
    f"{silver_dir}/stocks/data.parquet"
)


utils.save_to_parquet(
    df_intraday_bronze,
    f"{silver_dir}/intraday_values",
    ["only_date", "hour"]
)



# --- Procesamiento para la capa gold ---

# Leo los datos de la capa silver.
search_dir = f"{silver_dir}/stocks"
intraday_dir = f"{silver_dir}/intraday_values"

df_search_silver = utils.read_from_parquet(search_dir)
df_intraday_silver = utils.read_from_parquet(intraday_dir)

# Agregacion de columna symbol, conteo de instrumentos de inversion por pais y tipo (de df_search).

df_search_silver.groupby(
    ['country', 'stock_type']
).agg(
    {
        'symbol': 'count'
    }
).rename(
    columns={
        'symbol': 'investment_instrument_count'
    }
)


# Agregacion de columna symbol, calculo del volumen de negociacion total por symbol, valores promedio de inicio y cierre (df_intraday).

df_summary = df_intraday_silver.groupby(
                'symbol'
            ).agg(
                {
                    'trading_volume': 'sum',
                    'close_value': 'mean',
                    'open_value': 'mean',

                }
            ).rename(
                columns={
                    'trading_volume': 'trading_volume_sum',
                    'close_value': 'close_value_mean',
                    'open_value': 'open_value_mean'
                }
            )


# Agrego las columnas de df_summary a df_intraday.
df_intraday_silver = df_intraday_silver.merge(df_summary, on='symbol', how='left')

# Calculo de estimacion de la capitalizacion de mercado por symbol: la misma sera la cantidad de acciones negociadas por el precio
# de cierre.
df_intraday_silver['market_cap'] = df_intraday_silver['trading_volume_sum'] * df_intraday_silver['close_value_mean']
print(df_intraday_silver)
print(df_search_silver)


# Almacenamiento de dataframes enriquecidos en capa gold

gold_dir = "datalake/gold/stockdata_api"

utils.save_to_parquet(
    df_search_silver,
    f"{gold_dir}/stocks/data.parquet"
)


utils.save_to_parquet(
    df_intraday,
    f"{gold_dir}/intraday_values",
    ["only_date", "hour"]
)