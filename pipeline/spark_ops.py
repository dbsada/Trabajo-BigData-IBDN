import os
import logging
import subprocess
from utils.shell import sh
from utils.spark import spark_submit_train, spark_submit_predict
from config import DeployConfig

@sh
def _predict_delay_cmd(cfg):
    return f"docker exec {cfg.spark_container} {spark_submit_predict()}"

def predict_delay(cfg):
    logging.info("Launching prediction in Spark...")
    cmd = f"docker exec {cfg.spark_container} {spark_submit_predict()}"
    os.makedirs('logs', exist_ok=True)
    log_name = f'logs/flight_prediction_2.13-0.1.jar.log'
    log_file = open(log_name, 'w')
    logging.info(f'Launching service in background: {cmd} (Log: {log_name})')
    return subprocess.Popen(cmd, shell=True, cwd=cfg.project_home, stdout=log_file, stderr=log_file, start_new_session=True)

def train_model(cfg, extra_args=None):
    logging.info("Starting training Spark MLlib...")
    cmd = f"docker exec {cfg.spark_container} {spark_submit_train(extra_args)}"
    os.makedirs('logs', exist_ok=True)
    log_name = f'logs/train_model.log'
    log_file = open(log_name, 'w')
    logging.info(f'Launching training in background: {cmd} (Log: {log_name})')
    return subprocess.Popen(cmd, shell=True, cwd=cfg.project_home, stdout=log_file, stderr=log_file, start_new_session=True)

@sh
def show_prediction_logs(log_file):
    return f"less -S +G {log_file}"

def show_prediction_logs_wrapper(prediction_log=None):
    prediction_log = prediction_log or os.getenv('PREDICTION_LOG', 'logs/flight_prediction_2.13-0.1.jar.log')
    if os.path.exists(prediction_log):
        show_prediction_logs(prediction_log)
    else:
        print("Log file not found.")