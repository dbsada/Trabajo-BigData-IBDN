from flask import Flask, render_template
from flask_socketio import SocketIO
from dotenv import load_dotenv
import os

load_dotenv() 
FLASK_PORT = int(os.getenv("FLASK_PORT", 5001))

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

from routes.prediction import bp as prediction_bp
from routes.models import bp as models_bp
from routes.logs import bp as logs_bp

app.register_blueprint(prediction_bp)
app.register_blueprint(models_bp)
app.register_blueprint(logs_bp)

from kafka_handler import set_emit_function, start_consumers as _start_kafka
def _emit_saved(data):
    print(f"[Kafka] Emitting saved for {data.get('UUID','?')[:8]}")
    socketio.emit("saved", data)
set_emit_function(_emit_saved)

from routes.models import set_emit_function as set_training_emit
set_training_emit(lambda data: socketio.emit("training_update", data))

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    _start_kafka()
    socketio.run(app, host="0.0.0.0", port=FLASK_PORT, debug=True, use_reloader=False, allow_unsafe_werkzeug=True)