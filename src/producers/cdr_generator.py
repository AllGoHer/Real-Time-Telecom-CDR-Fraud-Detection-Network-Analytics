import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuración del productor
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
TOPIC_NAME = 'cdr_events'

# Inicializar el productor (enviamos datos como strings JSON)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Datos de prueba para simular realismo
COUNTRIES = ['ES', 'US', 'GB', 'DE', 'FR', 'NG', 'BR']
PREFIXES = {'ES': '+34', 'US': '+1', 'GB': '+44', 'DE': '+49', 'FR': '+33', 'NG': '+234', 'BR': '+55'}

def generate_cdr():
    country = random.choice(COUNTRIES)
    prefix = PREFIXES[country]
    
    # Simular un posible fraude: muchas llamadas cortas a números premium o internacionales
    is_fraud = random.random() < 0.1  # 10% de probabilidad de fraude
    duration = random.randint(1, 5) if is_fraud else random.randint(30, 600)
    
    return {
        "call_id": f"CDR-{int(time.time() * 1000)}",
        "caller": f"{prefix}{random.randint(600000000, 699999999)}",
        "callee": f"{prefix}{random.randint(700000000, 799999999)}",
        "duration_sec": duration,
        "country_code": country,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "is_suspicious": is_fraud  # Etiqueta oculta para validar nuestro detector de fraude después
    }

if __name__ == "__main__":
    print(f"🚀 Iniciando generador de CDRs. Enviando a {TOPIC_NAME}...")
    print("Presiona Ctrl+C para detener.")
    
    try:
        while True:
            cdr = generate_cdr()
            producer.send(TOPIC_NAME, value=cdr)
            print(f"✅ Enviado: {cdr['call_id']} | Duración: {cdr['duration_sec']}s | País: {cdr['country_code']}")
            time.sleep(0.5)  # 2 CDRs por segundo
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo generador...")
        producer.flush()
        producer.close()