import json
from confluent_kafka import Producer
from core.extract import get_live_flights
import logging

# This automatically names the logger after the file
logger = logging.getLogger(__name__)

class OpenSkyProducer:
    def __init__(self, bootstrap_servers, topic, bbox_info):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic
        self.bbox_info = bbox_info

    def delivery_report(self, err, msg):
        """ Callback called once for each message to check success. """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            pass

    def publish_to_bronze(self):
        # extract data
        flights = get_live_flights(self.bbox_info)
        
        # feed data to Kafka
        for flight in flights:
            payload = json.dumps(flight).encode('utf-8')
            self.producer.produce(
                self.topic, 
                value=payload, 
                callback=self.delivery_report 
            )
        
        self.producer.flush() 
        logger.info(f"Batch processed: {len(flights)} attempts.")