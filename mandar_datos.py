from kafka import KafkaProducer
import time
import random 
from datetime import datetime
import json
from realtime import DataCollectionRealTime


# data_collection = DataCollectionRealTime()
# empresas, ciks = data_collection.obtener_empresas('Real Estate')
# datos = data_collection.extraer_datos(empresas, ciks, 2024)
# data_collection.guardar_datos(datos)

producer = KafkaProducer(bootstrap_servers='192.168.80.34:9092')

# leer el archivo json y convertirlo en una lista de diccionarios
with open('data_realtime.json') as file:
    data = json.load(file)
    
# enviar cada diccionario como un mensaje al topic 'test'
for record in data:
    producer.send('real_estate', json.dumps(record).encode('utf-8'))
    time.sleep(0.001)
    print(f"Sent: {record}")
