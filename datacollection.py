from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
import requests
import fastavro
import pyorc
import io
import os

class DataCollection:
    def datos_historicos(self, ticker, inicio, fin): # Función para obtener los datos históricos de una empresa
        empresa = yf.Ticker(ticker)
        datos_historicos = empresa.history(start=inicio, end=fin)
        return datos_historicos

    def obtener_empresas(self, sector):
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
        empresas_year = []
        for symbol in empresas:
            try:
                inicio = f"{year}-01-01"
                fin = f"{year}-12-31"
                # Obtener y guardar los datos históricos para cada empresa
                datos = self.datos_historicos(symbol, inicio, fin)
                datos['Symbol'] = symbol
                datos['CIK'] = ciks[empresas.index(symbol)]
                # Convertir el índice a una columna
                empresas_year.append(datos)
            except Exception as e:
                print(f"No se pudieron obtener los datos de {symbol}: {e}")

        # Concatenar los DataFrames
        dataframe = pd.concat(empresas_year)
        # Transformar el índice a una columna
        dataframe.reset_index(inplace=True)
        # Transformar la columna Date a formato YYYY-MM-DD
        dataframe['Date'] = pd.to_datetime(dataframe['Date']).dt.strftime('%Y-%m-%d')
        # Alternativa: same_year['Date'] = same_year['Date'].astype(str).str.slice(0, 10)

        return dataframe

    def leer_ruta(self, archivo_texto):
        with open(archivo_texto, 'r') as file:
            ruta = file.readline().strip()

        if not os.path.exists(ruta):
            os.makedirs(ruta)

        return ruta

    def exportar_formato(self, year, formato, ruta, dataframe):
        if formato == 'excel': formato = 'xlsx'
        ruta_archivo = os.path.join(ruta, f'{year}.{formato}')
        
        if formato == 'avro':
            records_data = dataframe.to_dict(orient='records')
            schema = {
                'doc': 'DataFrame to Avro',
                'name': 'Data2018',
                'namespace': 'test', # ¿?
                'type': 'record', # ¿?
                'fields': [
                    {'name': 'Date', 'type': 'string'},
                    {'name': 'Open', 'type': 'double'},
                    {'name': 'High', 'type': 'double'},
                    {'name': 'Low', 'type': 'double'},
                    {'name': 'Close', 'type': 'double'},
                    {'name': 'Volume', 'type': 'double'},
                    {'name': 'Dividends', 'type': 'double'},
                    {'name': 'Stock Splits', 'type': 'double'},
                    {'name': 'Symbol', 'type': 'string'},
                    {'name': 'CIK', 'type': 'string'}
                ]
            }

            with open(ruta_archivo, 'wb') as file_avro:
                # La función writer necesita un objeto file y el esquema Avro
                fastavro.writer(file_avro, schema, records_data)
        
        elif formato == 'parquet':
            dataframe.to_parquet(ruta_archivo)

        elif formato == 'csv':
            dataframe.to_csv(ruta_archivo)
        
        elif formato == 'json':
            dataframe.to_json(ruta_archivo, orient='records')

        elif formato == 'orc':
            # Convertir el DataFrame de Pandas a un objeto 'bytes' en formato ORC, usando el esquema
            buffer = io.BytesIO()
            schema = 'struct<Date:string,Open:double,High:double,Low:double,Close:double,Volume:double,Dividends:double,StockSplits:double,Symbol:string,CIK:string>'
            writer = pyorc.Writer(buffer, schema)
            for row in dataframe.itertuples(index=False):
                writer.write(row)
            writer.close()

            with open(ruta_archivo, 'wb') as file_orc:
                file_orc.write(buffer.getvalue())

        elif formato == 'xlsx':
            dataframe.to_excel(ruta_archivo)
        
        print(f'Archivo {year}.{formato} exportado con éxito')