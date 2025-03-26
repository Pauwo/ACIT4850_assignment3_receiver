import connexion
from connexion import NoContent
import httpx 
import time
import yaml
import logging
import logging.config
import json
import datetime
from pykafka import KafkaClient

# Load the configuration file
with open('./receiver/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Load logging configuration from YAML
with open("./receiver/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

# Create a logger instance
logger = logging.getLogger('basicLogger')

kafka_config = app_config['events']
client = KafkaClient(hosts=f"{kafka_config['hostname']}:{kafka_config['port']}")  # Use config values
topic = client.topics[str.encode(kafka_config['topic'])]
producer = topic.get_sync_producer()

# Function for the flight schedule event
def report_flight_schedules(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id
    logger.info(f"Received flight schedule event (trace_id: {trace_id})")

    # Create Kafka message
    msg = {
        "type": "flight_schedule",  # Custom event type
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body  # Includes trace_id
    }
    msg_str = json.dumps(msg)
    
    # Send to Kafka
    producer.produce(msg_str.encode('utf-8'))
    logger.info(f"Produced flight_schedule event (trace_id: {trace_id})")

    return NoContent, 201 

# Function for the passenger check-in event
def record_passenger_checkin(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id
    logger.info(f"Received passenger check-in event (trace_id: {trace_id})")

    # Create Kafka message
    msg = {
        "type": "passenger_checkin",  # Custom event type
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body  # Includes trace_id
    }
    msg_str = json.dumps(msg)
    
    # Send to Kafka
    producer.produce(msg_str.encode('utf-8'))
    logger.info(f"Produced passenger_checkin event (trace_id: {trace_id})")

    return NoContent, 201

# Connexion app setup
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)  # Receiver service runs on port 8080