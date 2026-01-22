import time
import random
from kafka import KafkaProducer

try:
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    print("Conexión con Kafka exitosa")
except Exception as e:
    print(f"Error al conectar con kafka: {e}")
    exit()

frases = [
    "Docker empaqueta tus herramientas para que corran igual en cualquier PC.",
    "Kafka funciona como la tubería que transporta y guarda los mensajes.",
    "Spark es el motor que procesa y analiza esos datos rápidamente.",
    "Kafka envía los datos y Spark Streaming los procesa al instante.",
    "Docker Compose enciende todo tu sistema de datos con un solo comando."
]

print("Enviando mensajes a Kafka... Presiona Ctrl+C en el terminal para detener.")

try:
    while True:
        mensaje = random.choice(frases)
        
        producer.send('actividad-topic', mensaje.encode('utf-8'))
        
        print(f"Mensaje enviado: '{mensaje}'")
        
        time.sleep(random.uniform(1,3))

except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")

finally:
    print("Cerrando productor")
    producer.flush()
    producer.close()