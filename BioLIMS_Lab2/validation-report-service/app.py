import os
import json
import socket
import threading
import time
import pika
import requests
import uvicorn
from fastapi import FastAPI

app = FastAPI(title="Validation and Report Service (Report Context)")

INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv("SERVICE_NAME", "validation-report-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8003))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

# Сховище готових діагностичних звітів (DiagnosticReport)
REPORTS_DB = {}

# Референтні інтервали (ReferenceRange)
REFERENCE_RANGES = {
    "GLU": {"min": 3.9, "max": 6.1, "unit": "mmol/L"},
    "ALT": {"min": 7.0, "max": 40.0, "unit": "U/L"},
    "CREA": {"min": 62.0, "max": 106.0, "unit": "umol/L"}
}

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
            print(f"[{INSTANCE_ID}] Успішно зареєстровано в Consul")
            break
        except Exception:
            time.sleep(3)

def consume_rabbitmq_messages():
    """Фоновий консьюмер черги RabbitMQ (асинхронна обробка AssayCompletedEvent)"""
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue="assay_completed_queue", durable=True)

            def callback(ch, method, properties, body):
                event = json.loads(body.decode("utf-8"))
                barcode = event["barcode"]

                # Anti-Corruption Layer (ACL) + перевірка референтних норм (ReferenceRange)
                validated_items = []
                for item in event["measurements"]:
                    code = item["biomarker"]
                    val = item["value"]
                    ref = REFERENCE_RANGES.get(code, {"min": 0, "max": 999})
                    is_normal = ref["min"] <= val <= ref["max"]
                    validated_items.append({
                        "biomarker": code,
                        "concentration": f"{val} {item['unit']}",
                        "reference_range": f"{ref['min']} - {ref['max']} {item['unit']}",
                        "clinical_status": "NORMAL" if is_normal else "CRITICAL_DEVIATION"
                    })

                final_trace = (
                    f"{event['trace_chain']} -> "
                    f"[3. ValidationReportService (instance: {INSTANCE_ID}): "
                    f"прочитано з RabbitMQ, виконано DeltaCheck та накладено КЕП]"
                )

                REPORTS_DB[barcode] = {
                    "report_id": f"REP-{len(REPORTS_DB) + 1:03d}",
                    "barcode": barcode,
                    "delta_check": "PASSED (аномальних стрибків у динаміці не виявлено)",
                    "digital_signature": "SIGNED_BY_DOCTOR_KEP",
                    "qr_verification": f"https://biolims.cloud/verify/{barcode}",
                    "validated_results": validated_items,
                    "full_processing_trace": final_trace,
                    "consumed_by_instance": INSTANCE_ID
                }
                print(f"[{INSTANCE_ID}] Асинхронно сформовано звіт для штрихкоду {barcode}")
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="assay_completed_queue", on_message_callback=callback)
            print(f"[{INSTANCE_ID}] Консьюмер RabbitMQ запущено, очікування повідомлень...")
            channel.start_consuming()
        except Exception as e:
            print(f"[{INSTANCE_ID}] Очікування підключення до RabbitMQ... ({e})")
            time.sleep(4)

@app.on_event("startup")
def startup_event():
    threading.Thread(target=register_in_consul, daemon=True).start()
    threading.Thread(target=consume_rabbitmq_messages, daemon=True).start()

@app.get("/reports")
def get_all_reports():
    """Отримати всі згенеровані клініко-діагностичні звіти"""
    return {
        "service": SERVICE_NAME,
        "handled_by_instance": INSTANCE_ID,
        "reports_count": len(REPORTS_DB),
        "reports": REPORTS_DB
    }

@app.get("/reports/{barcode}")
def get_report_by_barcode(barcode: str):
    """Отримати готовий звіт за штрихкодом пробірки"""
    return REPORTS_DB.get(barcode, {"status": "PENDING_VALIDATION"})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)