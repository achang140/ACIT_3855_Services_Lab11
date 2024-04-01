import connexion 
from connexion import NoContent 
import os
import yaml 
import time 
import logging
import logging.config
import uuid
import datetime
import json
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


current_retry = 0
max_retries = app_config["events"]["max_retries"]

while current_retry < max_retries:
    try:
        logger.info(f"Trying to connect to Kafka. Current retry count: {current_retry}")
        client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
        # topic = client.topics[str.encode(app_config["events"]["topic"])]
        # producer = topic.get_sync_producer()

        # First Topic events 
        first_topic = client.topics[str.encode(app_config["events"]["topics"][0])]
        first_producer = first_topic.get_sync_producer()

        # Second Topic event_log 
        second_topic = client.topics[str.encode(app_config["events"]["topics"][1])]
        second_producer = second_topic.get_sync_producer()

        break 

    except:
        logger.error("Connection failed.")
        time.sleep(app_config["events"]["sleep_time"])
        current_retry += 1


def load(producer_two):
    """ Send Message to Kafka """

    if producer_two is None:
        logger.error("Producer does not exist")
    else:
        ready_msg = {
            "message_info": "Receiver service successfully started and connected to Kafka. Ready to receive messages on RESTful API.",
            "message_code": "0001"
        }
        ready_msg_str = json.dumps(ready_msg)
        producer_two.produce(ready_msg_str.encode('utf-8'))


def book_hotel_room(body):
    """ Receives a hotel room booking event """
    # global first_producer

    trace_id = uuid.uuid4()
    body["trace_id"] = str(trace_id)

    logger.info("Received event Hotel Room Booking request with a trace id of %s", body["trace_id"])

    # headers = { "content-type": "application/json" }
    # response = requests.post(app_config["eventstore1"]["url"], json=body, headers=headers) # requests.post(url=event_one_url, json=body, headers=headers)
    # logger.info(f"Returned event Hotel Room Booking response (Id: ${body['trace_id']}) with status ${response.status_code}")
    
    # client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    # topic = client.topics[str.encode(app_config["events"]["topic"])]
    # producer = topic.get_sync_producer()
    # producer.produce(msg_str.encode('utf-8'))
    
    # First Topic (events)
    msg = {
        "type": "hotel_room",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body
    }
    msg_str = json.dumps(msg)
    first_producer.produce(msg_str.encode('utf-8'))

    logger.info(f"Returned event Hotel Room Booking response (Id: ${body['trace_id']}) with status 201")

    # return NoContent, response.status_code
    return NoContent, 201

def book_hotel_activity(body):
    """ Receives a hotel activity reservation event """
    # global first_producer

    trace_id = uuid.uuid4()
    body["trace_id"] = str(trace_id)


    logger.info("Received event Hotel Activity Booking request with a trace id of %s", body["trace_id"])

    # headers = { "content-type": "application/json" }
    # response = requests.post(app_config["eventstore2"]["url"], json=body, headers=headers) # requests.post(url=event_two_url, json=body, headers=headers)
    # logger.info("Returned event Hotel Activity Booking response (Id: %s) with status %d", body["trace_id"], response.status_code)

    # client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    # topic = client.topics[str.encode(app_config["events"]["topic"])]
    # producer = topic.get_sync_producer()
    # producer.produce(msg_str.encode('utf-8'))
    
    # First Topic (events)
    msg = {
        "type": "hotel_activity",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body
    }
    msg_str = json.dumps(msg)
    first_producer.produce(msg_str.encode('utf-8'))

    logger.info("Returned event Hotel Activity Booking response (Id: %s) with status %d", body["trace_id"], 201)

    # return NoContent, response.status_code
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir="")
# app.add_api("openapi.yaml", strict_validation=True, validate_responses=True) 
app.add_api("openapi.yml", base_path="/receiver", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    load(second_producer)
    app.run(port=8080)

