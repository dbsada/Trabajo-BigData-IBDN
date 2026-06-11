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

from kafka_handler import set_emit_function, start_consumers
set_emit_function(lambda data: socketio.server.emit("saved", data))
start_consumers()

from routes.models import set_emit_function as set_training_emit
set_training_emit(lambda data: socketio.server.emit("training_update", data))

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=FLASK_PORT, debug=True, allow_unsafe_werkzeug=True)