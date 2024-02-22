from kafka import KafkaProducer
import time
import random 
from datetime import datetime, date
import json
from DataCollection import DataCollection

data_collection = DataCollection()
empresas, ciks = data_collection.obtener_empresas('Real Estate')
datos = data_collection.extraer_datos(empresas, ciks, 2024, "2024-01-01", date.today())
data = datos.to_dict(orient="records")
producer = KafkaProducer(bootstrap_servers='192.168.80.34:9092')
for record in data:
    producer.send('real_estate', json.dumps(record).encode('utf-8'))
    time.sleep(0.001)
    print(f"Sent: {record}")