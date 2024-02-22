from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
import requests
import fastavro
import pyorc
import io
import os
from datetime import date

class DataCollectionRealTime:
    def datos_historicos(self, ticker, inicio, fin): # Función para obtener los datos históricos de una empresa
        """
        Esta función obtiene los datos de una empresa concreta en un rango de fechas determinado.

        Args:
            ticker (str): Símbolo de la empresa
            inicio (str): Fecha de inicio en formato YYYY-MM-DD
            fin (str): Fecha de fin en formato YYYY-MM-DD
        
        Returns:
            datos_historicos (DataFrame): Datos de la empresa
        """

        empresa = yf.Ticker(ticker)
        datos_historicos = empresa.history(start=inicio, end=fin)
        return datos_historicos

    def obtener_empresas(self, sector):
        """
        Esta función obtiene los símbolos y CIKs de las empresas de un sector determinado.

        Args:
            sector (str): Sector de las empresas
        
        Returns:
            empresas_re (list): Lista con los símbolos de las empresas
            ciks (list): Lista con los CIKs de las empresas
        """

        # URL de la página de Wikipedia
        url_sp500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        # Obtener la página
        response = requests.get(url_sp500)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Encontrar la tabla de empresas
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

        # Crear un DataFrame
        df = pd.DataFrame(data, columns=headers)
        # Filtrar por empresas del sector, en este caso será Real Estate (re)
        df_re = df[df['GICS Sector'] == sector]
        # Obtener los símbolos de las empresas de real estate
        empresas_re = df_re['Symbol'].tolist()
        # Obtener el CIK de cada empresa
        ciks = df_re['CIK'].tolist()

        return empresas_re, ciks


    def extraer_datos(self, empresas, ciks, year):
        datos_empresas = []
        for symbol in empresas:
            try:
                inicio = f"2024-01-01"
                fin = date.today().strftime("%Y-%m-%d")
                # Obtener y guardar los datos históricos para cada empresa
                datos = self.datos_historicos(symbol, inicio, fin)
                datos['Symbol'] = symbol
                datos['CIK'] = ciks[empresas.index(symbol)]
                # Convertir el índice a una columna
                datos_empresas.append(datos)
            except Exception as e:
                print(f"No se pudieron obtener los datos de {symbol}: {e}")
        
        # Concatenar los DataFrames
        dataframe = pd.concat(datos_empresas, axis=0)
        # Transformar el índice a una columna
        dataframe.reset_index(inplace=True)
        # Transformar la columna Date a formato YYYY-MM-DD
        dataframe['Date'] = pd.to_datetime(dataframe['Date']).dt.strftime('%Y-%m-%d')
    
        return dataframe

    def guardar_datos(self, dataframe):
        # Guardamos el dataframe como JSON
        dataframe.to_json('datos_realtime.json', orient='records')


if __name__ == "__main__":
    # Instanciar la clase
    data_collection = DataCollectionRealTime()
    # Obtener los símbolos y CIKs de las empresas de un sector
    empresas, ciks = data_collection.obtener_empresas('Real Estate')
    # Extraer los datos históricos de las empresas
    datos = data_collection.extraer_datos(empresas, ciks, 2024)
    # Guardar los datos en un archivo
    data_collection.guardar_datos(datos)