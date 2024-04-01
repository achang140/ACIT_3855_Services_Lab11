import connexion
from connexion import FlaskApp
from flask_cors import CORS, cross_origin

import os 
import yaml 
import json 
import logging
import logging.config
from pykafka import KafkaClient

# with open('app_conf.yml', 'r') as f: 
#     app_config = yaml.safe_load(f.read())

# with open('log_conf.yml', 'r') as f: 
#     log_config = yaml.safe_load(f.read()) 
#     logging.config.dictConfig(log_config)

# logger = logging.getLogger('basicLogger')

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

# External Logging Configuration
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)
logger = logging.getLogger('basicLogger')
logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)


def get_hotel_room(index):
    """ Get Hotel Room reservations in History """
    hostname = "%s:%d" % (app_config["events"]["hostname"], app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]

    # Here we reset the offset on start so that we retrieve messages at the beginning of the message queue.
    # To prevent the for loop from blocking, we set the timeout to 100ms. 
    # There is a risk that this loop never stops if the index is large and messages are constantly being received!
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=2000)
    logger.info("Retrieving Hotel Room at index %d" % index)
    hotel_room_list = []
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            # Find the event at the index you want and return code 200 i.e., return event, 200
            if msg["type"] == "hotel_room":
                hotel_room_list.append(msg)

        logger.info(f"Hotel Room in {index}: {hotel_room_list[index]}")
        return hotel_room_list[index], 200
    except:
        logger.error("No more messages found")

    logger.error("Could not find Hotel Room at index %d" % index)
    return {"message": "Not Found"}, 404

def get_hotel_activity(index):
    """ Get Hotel Activity reservations in History """
    hostname = "%s:%d" % (app_config["events"]["hostname"], app_config["events"]["port"])
    client = KafkaClient(hosts=hostname) 
    topic = client.topics[str.encode(app_config["events"]["topic"])]

    # Here we reset the offset on start so that we retrieve messages at the beginning of the message queue.
    # To prevent the for loop from blocking, we set the timeout to 100ms. 
    # There is a risk that this loop never stops if the index is large and messages are constantly being received!
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=2000)
    logger.info("Retrieving Hotel Activity at index %d" % index)
    hotel_activity_list = []
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            # Find the event at the index you want and return code 200 i.e., return event, 200
            if msg["type"] == "hotel_activity":
                hotel_activity_list.append(msg)

        logger.info(f"Hotel Activity in {index}: {hotel_activity_list[index]}")
        return hotel_activity_list[index], 200
    except:
        logger.error("No more messages found")

    logger.error("Could not find Hotel Activity at index %d" % index)
    return {"message": "Not Found"}, 404


app = connexion.FlaskApp(__name__, specification_dir="")
# app.add_api("openapi.yaml", strict_validation=True, validate_responses=True) 
app.add_api("openapi.yml", base_path="/audit_log", strict_validation=True, validate_responses=True)

# CORS(app.app)
# app.app.config["CORS_HEADERS"] = "Content-Type"

if "TARGET_ENV" not in os.environ or os.environ["TARGET_ENV"] != "test":
    CORS(app.app)
    app.app.config['CORS_HEADERS'] = 'Content-Type'

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8110)
