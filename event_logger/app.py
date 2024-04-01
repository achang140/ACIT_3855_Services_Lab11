import connexion 
from connexion import FlaskApp
from flask_cors import CORS, cross_origin

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

from base import Base
from event_stats import EventStats

import os 
import yaml
import json
import time 
import datetime
import logging
import logging.config
from threading import Thread
from pykafka import KafkaClient
from pykafka.common import OffsetType


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


db_file_path = app_config["datastore"]["filename"]

# Connect to the database (db name: event_stats.sqlite)
DB_ENGINE = create_engine("sqlite:///%s" % app_config["datastore"]["filename"])

if not os.path.exists(db_file_path):
    Base.metadata.create_all(DB_ENGINE)

Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)



def process_messages():
    """ 
    Process service event messages 
    
    - Connect to Kafka 
    - Set up a consumer  
    - Consume event log messages (receiver, storage, and processing services) from the event_log topic and save them to 1) log file 2) SQLite database 
    """

    hostname = "%s:%d" % (app_config["events"]["hostname"], app_config["events"]["port"])

    current_retry = 0
    max_retries = app_config["events"]["max_retries"]

    while current_retry < max_retries:
        try:
            logger.info(f"Trying to connect to Kafka. Current retry count: {current_retry}")
            client = KafkaClient(hosts=hostname) 
            topic = client.topics[str.encode(app_config["events"]["topic"])]
            break 
        except:
            logger.error("Connection failed.")
            time.sleep(app_config["events"]["sleep_time"])
            current_retry += 1

    # It should be setup as a consumer for the event_log topic. Similar to the Storage service and the events topic, it should consume new messages and keep track of its offset.
    # Create a consume on a consumer group, that only reads new messages (uncommitted messages) when the service re-starts 
    # (i.e., it doesn't read all the old messages from the history in the message queue).
    
    consumer = topic.get_simple_consumer(consumer_group=b'event_log_group',
                                         reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.LATEST)
    
    # When it consumes one of the event log messages from the topic, it should write it to 1) it log file and 2) a database.

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)

        msg_info = msg["message_info"]
        msg_code = msg["message_code"]

        session = DB_SESSION()

        stat = EventStats(
            message_info=msg_info,
            message_code=msg_code,
            last_updated=datetime.datetime.now() 
        )

        # 1) Log File 
        logger.debug("Stored new event log message from the %s topic.\nmessage_info=%s, message_code=%s, last_updated=%s" % (app_config['events']['topic'], stat.message_info, stat.message_code, stat.last_updated))
        
        # 2) SQLite DB 
        # Unique ID of the record will auto-increment
        session.add(stat)
        session.commit() 
        session.close() 

        # Commit the new message as being read
        consumer.commit_offsets()



def get_event_stats():
    """ Receive event statistics from the EventStats table within the event_stats.sqlite database """

    # Log an INFO message indicating request has started
    logger.info("Request Started")

    # Read in the current statistics from the SQLite database (i.e., the row with the most recent last_update datetime stamp.
    session = DB_SESSION() 

    # Retrieve data from the EventStats table, select the message_code column, count number of messages for each message code, and group by the message_code column 
    stats_counts = session.query(EventStats.message_code, func.count(EventStats.message_code)).group_by(EventStats.message_code).all()

    # If no stats exist, log an ERROR message and return 404 and the message “Statistics do not exist” OR return empty/default statistics
    if not stats_counts:
        logger.error("Statistics do not exist")
        return "Statistics do not exist", 404

    # Default 
    # Convert them as necessary into a new Python dictionary such that the structure matches that of your response defined in the openapi.yaml file.
    required_message_codes = ["0001", "0002", "0003", "0004"]
    statistics = {code: 0 for code in required_message_codes} 

    # Update the counts based on the actual statistics obtained from the SQLite database
    statistics.update({code: count for code, count in stats_counts})

    # Log a DEBUG message with the contents of the Python Dictionary
    logger.debug(statistics)

    # Log an INFO message indicating request has completed
    logger.info("Request Completed!")

    session.close() 

    # Return the Python dictionary as the context and 200 as the response code
    return statistics, 200 



app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True) 

CORS(app.app)
app.app.config["CORS_HEADERS"] = "Content-Type"



if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(host="0.0.0.0", port=8120)