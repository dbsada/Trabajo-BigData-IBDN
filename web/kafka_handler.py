"""
Módulo de comunicación con Kafka: enviar predicciones y recibir respuestas.
"""

import json
import os
import threading
import time
import uuid as uuid_module

from kafka import KafkaConsumer, KafkaProducer

KAFKA = os.getenv("KAFKA", "kafka:9092")
TOPIC_REQUEST = os.getenv("TOPIC_IN", "request")
TOPIC_RESPONSE = os.getenv("TOPIC_OUT", "response")

_producer = None
_emit_saved = lambda data: None

def _get_producer():
    """Crea el KafkaProducer la primera vez que se necesita."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[KAFKA],
            max_block_ms=10000,
        )
    return _producer


def send_prediction(form_data: dict, model_ids: list) -> str:
    """
    Convierte los datos del formulario en un mensaje JSON
    y lo envía a Kafka. Devuelve el UUID para que la UI haga polling.

    form_data: dict con Origin, Dest, Carrier, DepDelay, FlightDate, FlightNum
    model_ids: lista de IDs de modelos a usar (ej: ["abc...", "def..."])
    """
    import utils

    prediction = dict(form_data)

    distance = utils.get_flight_distance(form_data.get("Origin"), form_data.get("Dest"))
    date_fields = utils.get_regression_date_args(form_data.get("FlightDate", ""))
    unique_id = str(uuid_module.uuid4())
    
    prediction["Distance"] = distance
    prediction.update(date_fields)
    prediction["Timestamp"] = utils.get_current_timestamp()
    prediction["UUID"] = unique_id
    prediction["model_ids"] = model_ids

    for key in ["FlightNum", "Carrier"]:
        if prediction.get(key) is None:
            prediction[key] = ""

    message_bytes = json.dumps(prediction).encode("utf-8")
    producer = _get_producer()
    future = producer.send(TOPIC_REQUEST, message_bytes)
    future.get(timeout=2)

    return unique_id


def _save_to_cassandra(data: dict):
    """Guarda una predicción en Cassandra."""
    import utils

    session = utils.get_cassandra_session()
    if session is None:
        raise Exception("No se pudo conectar a Cassandra")

    session.execute(
        """
        INSERT INTO agile_data_science.flight_delay_ml_response (
            uuid, prediction, model_id, origin, dest, dep_delay, carrier,
            flight_date, flight_num, distance, route,
            day_of_year, day_of_month, day_of_week, timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            data.get("UUID"),
            int(data.get("Prediction", 0)),
            str(data.get("model_id", "")),
            data.get("Origin"),
            data.get("Dest"),
            float(data.get("DepDelay", 0.0)),
            data.get("Carrier"),
            data.get("FlightDate"),
            data.get("FlightNum"),
            data.get("Distance"),
            data.get("Route"),
            int(data.get("DayOfYear", 0)),
            int(data.get("DayOfMonth", 0)),
            int(data.get("DayOfWeek", 0)),
            str(data.get("Timestamp", "")),
        ),
    )


def _response_listener():
    """
    Hilo que escucha el topic 'response'
    y guarda los resultados en Cassandra y emite vía SocketIO a la UI.
    """
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_RESPONSE,
                bootstrap_servers=[KAFKA],
                auto_offset_reset="latest",
                group_id="predictor",
                max_poll_interval_ms=300000,
                fetch_max_wait_ms=100,
                max_poll_records=1,
            )
            for msg in consumer:
                try:
                    data = json.loads(msg.value.decode("utf-8"))
                    if not isinstance(data, dict) or "UUID" not in data:
                        continue

                    # Guardar en Cassandra
                    _save_to_cassandra(data)

                    # Emitir vía SocketIO (se pasa desde app.py)
                    _emit_saved(data)

                except Exception as e:
                    print(f"[Kafka] Error procesando mensaje: {e}")

        except Exception as e:
            print(f"[Kafka] Error de conexión: {e}. Reintentando en 5s...")
            time.sleep(5)


def set_emit_function(fn):
    """
    Permite a app.py inyectar la función de emisión SocketIO.
    Uso: set_emit_function(lambda data: socketio.emit('saved', data))
    """
    global _emit_saved
    _emit_saved = fn


def start_consumers():
    """Lanza el hilo consumidor de respuestas."""
    thread = threading.Thread(target=_response_listener, daemon=True)
    thread.start()