import requests
import pandas as pd
import os


def get_data(base_url, endpoint, params=None, headers=None):
  """
  Se define una funcion que dados como parametros de entrada la url base de la
  API y un endpoint de la misma, realiza una peticion GET y devuelve el json de
  respuesta a dicha solicitud.

  Parametros:
  base_url(str): URL base de la API
  endpoint(str): endpoint al que se realizara la solicitud
  params(dict): parametros de consulta que se enviaran en la solicitud
  headers(dict): encabezados que se enviaran en la solicitud

  Retorna:
  dict: JSON de respuesta de la API
  """

  try:
    endpoint_url = f'{base_url}/{endpoint}'
    response = requests.get(endpoint_url,params=params,headers=headers)
    response.raise_for_status()

    try:
      data = response.json()['data']
    except:
      print("Formato de response inesperado")
      return None
    return data

  except requests.exceptions.RequestException as err:
    print(f"La peticion ha fallado. Codigo de error: {err}")
    return None


def build_table(json_data):
  """
  Esta funcion dado un json construye un dataframe de Pandas con las keys
  como columnas y los valores como registros.

  Parametros:
  json_data(dict): JSON obtenido de la API
  """
  try:
    df = pd.json_normalize(json_data)
    return df
  except:
    print("Los datos no se encuentran en el formato esperado.")
    return None
  

def save_to_parquet(df, output_path, partition_cols=None):
    """
    Recibe un dataframe, se recomienda que haya sido convertido a un formato tabular,
    y lo guarda en formato parquet.

    Parametros:
    df (pd.DataFrame). Dataframe a guardar.
    output_path (str). Ruta donde se guardará el archivo. Si no existe, se creará.
    partition_cols (list o str). Columna/s por las cuales particionar los datos.
    """

    # Crear el directorio si no existe
    directory = os.path.dirname(output_path)
    if directory and not os.path.exists(directory):
      os.makedirs(directory)

    df.to_parquet(
      output_path,
      engine="fastparquet",
      partition_cols=partition_cols
    )
    

def read_from_parquet(dir):
    """
    Recibe un directorio. Lee los archivos parquet y los guarda en un dataframe.
    Retorna el dataframe.

    """
    try:
      df = pd.read_parquet(
        dir, 
        engine='fastparquet'
      )
      return df

    except:
      print("No se puede acceder a los archivos.")
      return None
