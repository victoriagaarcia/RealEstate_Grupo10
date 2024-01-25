import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

# Función para obtener los datos históricos de una empresa
def datos_historicos(ticker, inicio, fin):
    empresa = yf.Ticker(ticker)
    datos_historicos = empresa.history(start=inicio, end=fin)
    return datos_historicos

# URL de la página de Wikipedia
url_sp500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Obtener la página
response = requests.get(url_sp500)
soup = BeautifulSoup(response.text, 'html.parser')

# Encontrar la tabla correcta
tabla = soup.find('table', {'class': 'wikitable sortable'})

# Extraer los encabezados de la tabla
headers = []
for header in tabla.find_all('th'):
    headers.append(header.text.strip())

# Extraer las filas de la tabla
rows = tabla.find_all('tr')

# Lista para almacenar los datos
data = []

# Extraer los datos de cada fila
for row in rows:
    cols = row.find_all('td')
    if cols:
        data.append([col.text.strip() for col in cols])

# Crear un DataFrame de pandas
df = pd.DataFrame(data, columns=headers)

# Filtrar por empresas de real estate
df_re = df[df['GICS Sector'] == 'Real Estate']

# Obtener los símbolos de las empresas de real estate
empresas_re = df_re['Symbol'].tolist()

# Fechas de inicio y fin
# inicio = "2018-01-01"
# fin = "2024-01-01"

years = [2018, 2019, 2020, 2021, 2022, 2023]
formatos = ['csv', 'json', 'excel', 'parquet', 'avro', 'orc']

datos_df = []

for year in years:
    lista_df = []
    for symbol in empresas_re:
        try:
            inicio = f"{year}-01-01"
            fin = f"{year}-12-31"
            # Obtener y guardar los datos históricos para cada empresa
            datos = datos_historicos(symbol, inicio, fin)
            lista_df.append(datos)
            # Añadir ID de la empresa!
        except Exception as e:
            print(f"No se pudieron obtener los datos de {symbol}: {e}")

    # Concatenar los DataFrames
    datos_df.append(pd.concat(lista_df))

# 2018 a formato avro!

# 2019 a formato parquet
datos_df[1].to_parquet('2019.parquet')

# 2020 a formato csv
datos_df[2].to_csv('2020.csv')

# 2021 a formato json!

# 2022 a formato orc!

# 2023 a formato excel!