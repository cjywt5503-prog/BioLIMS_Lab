import os
import json
import socket
import threading
import time
import pika
import requests
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Assay Execution Service (Assay Context)")

INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv("SERVICE_NAME", "assay-execution-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8002))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-sample-service:8001")

class AssayRunRequest(BaseModel):
    barcode: str = "CODE128-1001"
    initial_message: str = "Запит від біохімічного аналізатора"

def register_in_consul():
    url = f"http://{CONSUL_HOST}:8500/v1/agent/service/register"
    payload = {
        "ID": f"{SERVICE_NAME}-{INSTANCE_ID}",
        "Name": SERVICE_NAME,
        "Address": INSTANCE_ID,
        "Port": SERVICE_PORT
    }
    for _ in range(10):
        try:
            requests.put(url, json=payload, timeout=3)
            print(f"[{INSTANCE_ID}] Зареєстровано")
            break
        except Exception:
            time.sleep(3)

def discover_service(service_name: str):
    try:
        # Змінено на /health і додано ?passing=true для отримання лише "живих" реплік
        resp = requests.get(f"http://{CONSUL_HOST}:8500/v1/health/service/{service_name}?passing=true", timeout=2)
        instances = resp.json()
        
        return [f"{item['Service']['Address']}:{item['Service']['Port']}" for item in instances]
    
    except Exception as e:
        print(f"Помилка Service Discovery: {e}")
        return []

def publish_assay_completed_event(event_payload: dict):
    # Асинхронна публікація доменної події AssayCompletedEvent у RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue="assay_completed_queue", durable=True)
    channel.basic_publish(
        exchange="",
        routing_key="assay_completed_queue",
        body=json.dumps(event_payload, ensure_ascii=False),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()

@app.on_event("startup")
def startup_event():
    threading.Thread(target=register_in_consul, daemon=True).start()

@app.post("/assays/run")
def run_assay(req: AssayRunRequest):
    """
    Демонстрація СИНХРОННОЇ та АСИНХРОННОЇ комунікації:
    1. Знаходить order-sample-service через Discovery і робить синхронний HTTP-запит.
    2. Дописує результати фотометричного вимірювання у ланцюжок trace_chain.
    3. Відправляє подію AssayCompletedEvent у чергу RabbitMQ.
    """
    # Крок 1: Перевірка доступних інстансів через Service Discovery (Consul)
    discovered_nodes = discover_service("order-sample-service")

    # Крок 2: Синхронний HTTP-виклик до order-sample-service (Connection: close для Round-Robin балансування)
    sync_url = f"{ORDER_SERVICE_URL}/orders/by-barcode/{req.barcode}"
    sync_resp = requests.get(
        sync_url,
        params={"client_note": req.initial_message},
        headers={"Connection": "close"},
        timeout=5
    ).json()

    # Крок 3: Імітація фотометричного поглинання (OpticalDensity) та розрахунок концентрацій за калібратором
    measurements = [
        {"biomarker": "GLU", "optical_density": 0.412, "value": 5.4, "unit": "mmol/L"},
        {"biomarker": "ALT", "optical_density": 0.195, "value": 28.0, "unit": "U/L"},
        {"biomarker": "CREA", "optical_density": 0.310, "value": 82.5, "unit": "umol/L"}
    ]

    # Дописуємо повідомлення від поточного мікросервісу до рядка з першого сервісу
    full_trace = (
        f"{sync_resp['trace_chain']} -> "
        f"[2. AssayService (instance: {INSTANCE_ID}): виміряна OpticalDensity, розраховані концентрації]"
    )

    # Крок 4: Асинхронне відправлення події AssayCompletedEvent у RabbitMQ
    event_data = {
        "event_type": "AssayCompletedEvent",
        "barcode": req.barcode,
        "order_instance": sync_resp["handled_by_order_instance"],
        "assay_instance": INSTANCE_ID,
        "measurements": measurements,
        "trace_chain": full_trace
    }
    publish_assay_completed_event(event_data)

    return {
        "status": "ASSAY_COMPLETED_AND_EVENT_PUBLISHED",
        "consul_discovered_order_instances": discovered_nodes,
        "handled_by_order_instance": sync_resp["handled_by_order_instance"],
        "handled_by_assay_instance": INSTANCE_ID,
        "trace_chain": full_trace,
        "measurements": measurements,
        "async_broker_queue": "assay_completed_queue (RabbitMQ)"
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)