from datacollection import DataCollection
import sys

if __name__ == '__main__':
    try:
        # Inicializar la clase DataCollection
        datacollector = DataCollection()

        # Controlar si no se indican argumentos
        if len(sys.argv) < 3:
            raise ValueError('No se han especificado los argumentos necesarios, por favor introduzca un año y un formato')

        # Obtener los argumentos de la línea de comandos
        year = sys.argv[1]
        formato = sys.argv[2]

        if year not in ['2018', '2019', '2020', '2021', '2022', '2023']:
            raise ValueError('No se contienen datos para el año especificado, por favor introduzca un año entre 2018 y 2023')
        elif formato not in ['csv', 'json', 'excel', 'parquet', 'avro', 'orc']:
            raise ValueError('El formato especificado no es válido, por favor introduzca un formato entre csv, json, excel, parquet, avro y orc')

        # Transformamos el año a entero
        year = int(year)

        # Obtener las empresas relativas al sector de real estate
        empresas, ciks = datacollector.obtener_empresas('Real Estate')

        # Obtener la ruta del archivo de texto
        ruta = datacollector.leer_ruta('ruta.txt')

        # Obtener los datos históricos de las empresas
        dataframe = datacollector.extraer_datos(empresas, ciks, year)

        # Exportar los datos en el formato especificado
        datacollector.exportar_formato(year, formato, ruta, dataframe)
    
    except Exception as e:
        print(f'Error: {e}')