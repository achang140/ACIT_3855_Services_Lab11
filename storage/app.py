import connexion 
from connexion import NoContent 

from sqlalchemy import create_engine
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from base import Base
from hotel_room import HotelRoom
from hotel_activity import HotelActivity

import os 
import yaml
import json 
import time 
import logging
import logging.config
import datetime
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread

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

DB_ENGINE = create_engine(
    f'mysql+pymysql://{app_config["datastore"]["user"]}:{app_config["datastore"]["password"]}@{app_config["datastore"]["hostname"]}:{app_config["datastore"]["port"]}/{app_config["datastore"]["db"]}',
    pool_size=5, 
    pool_recycle=3600, 
    pool_pre_ping=True 
    )

Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def process_messages():
    """ Process event messages """

    print("Process Messages Function")
    
    hostname = "%s:%d" % (app_config["events"]["hostname"], app_config["events"]["port"])

    current_retry = 0
    max_retries = app_config["events"]["max_retries"]

    while current_retry < max_retries:
        try:
            logger.info(f"Trying to connect to Kafka. Current retry count: {current_retry}")
            client = KafkaClient(hosts=hostname) 
            # topic = client.topics[str.encode(app_config["events"]["topic"])]
            
            # First Topic events 
            first_topic = client.topics[str.encode(app_config["events"]["topics"][0])]

            # Second Topic event_log 
            second_topic = client.topics[str.encode(app_config["events"]["topics"][1])]
            second_producer = second_topic.get_sync_producer()
            ready_msg = {
                "message_info": f"Storage service successfully started and connected to Kafka. Ready to consume messages from the {app_config['events']['topics'][1]} topic.",
                "message_code": "0002"
            }
            ready_msg_str = json.dumps(ready_msg)
            second_producer.produce(ready_msg_str.encode('utf-8'))

            break 

        except:
            logger.error("Connection failed.")
            time.sleep(app_config["events"]["sleep_time"])
            current_retry += 1

    print("Pass One!")

    # Create a consume on a consumer group, that only reads new messages (uncommitted messages) when the service re-starts 
    # (i.e., it doesn't read all the old messages from the history in the message queue).
    
    # consumer = topic.get_simple_consumer(consumer_group=b'event_group',
    #                                      reset_offset_on_start=False,
    #                                      auto_offset_reset=OffsetType.LATEST)
    
    consumer = first_topic.get_simple_consumer(consumer_group=b'event_group',
                                         reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.LATEST)
    
    print("Pass Two!")

    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]
        # print(payload)


        if msg["type"] == "hotel_room":  # Change this to your event type
            # Store the event1 (i.e., the payload) to the DB
            session = DB_SESSION() 
            hotel_room = HotelRoom(payload["hotel_id"],
                                   payload["customer_id"],
                                   payload["room_id"],
                                   payload["room_type"],
                                   payload["num_of_people"],
                                   payload["check_in_date"],
                                   payload["check_out_date"],
                                   payload["timestamp"],
                                   payload["trace_id"])
            session.add(hotel_room)
            logger.debug(f"Stored event Hotel Room Booking request with a trace id of {payload['trace_id']}")
            session.commit()
            session.close()
                        
        elif msg["type"] == "hotel_activity":  # Change this to your event type
            # Store the event2 (i.e., the payload) to the DB
            session = DB_SESSION()
            hotel_activity = HotelActivity(payload["hotel_id"],
                                           payload["customer_id"],
                                           payload["activity_id"],
                                           payload["activity_name"],
                                           payload["num_of_people"],
                                           payload["reservation_date"],
                                           payload["timestamp"],
                                           payload["trace_id"])
            session.add(hotel_activity)
            logger.debug(f"Stored event Hotel Activity Booking request with a trace id of {payload['trace_id']}")
            session.commit()
            session.close()

        # Commit the new message as being read
        consumer.commit_offsets()

def get_hotel_room(start_timestamp, end_timestamp):
    """ Gets new hotel room reservations between the start and end timestamps """
    
    session = DB_SESSION() 

    logger.info(f'Connecting to DB. Hostname: {app_config["datastore"]["hostname"]}, Port:{app_config["datastore"]["port"]}')

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    # print(start_timestamp_datetime)
    # print(end_timestamp_datetime)
    results = session.query(HotelRoom).filter(and_(HotelRoom.date_created >= start_timestamp_datetime, HotelRoom.date_created < end_timestamp_datetime))
    # print("After query")
    results_list = []

    # print(results)

    for reservation in results:
        # print(reservation.to_dict())
        results_list.append(reservation.to_dict())

    logger.debug("Query for Hotel Room Reservations after %s returns %d results" % (start_timestamp, len(results_list)))
    session.close()

    return results_list, 200

def get_hotel_activity(start_timestamp, end_timestamp):
    """ Gets new hotel activity reservations between the start and end timestamps """

    session = DB_SESSION() 

    logger.info(f'Connecting to DB. Hostname: {app_config["datastore"]["hostname"]}, Port:{app_config["datastore"]["port"]}')

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    results = session.query(HotelActivity).filter(and_(HotelActivity.date_created >= start_timestamp_datetime, HotelActivity.date_created < end_timestamp_datetime))
    results_list = []

    for reservation in results:
        results_list.append(reservation.to_dict())

    logger.debug("Query for Hotel Activity Reservations after %s returns %d results" % (start_timestamp, len(results_list)))
    session.close()

    return results_list, 200

app = connexion.FlaskApp(__name__, specification_dir="")
# app.add_api("openapi.yaml", strict_validation=True, validate_responses=True) 
app.add_api("openapi.yml", base_path="/storage", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)
