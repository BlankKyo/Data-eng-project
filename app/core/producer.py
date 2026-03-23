# core/producer.py
import json
from confluent_kafka import Producer
from core.extract import get_live_flights
import logging

# init logger
logger = logging.getLogger(__name__)

class OpenSkyProducer:
    """Handles the ingestion of live flight data from OpenSky API into Kafka.

    This class acts as the 'Bronze' layer ingestion engine. It fetches raw 
    state vectors for a specific bounding box and streams them as 
    individual JSON messages into a Kafka topic.

    Attributes:
        producer (Producer): Confluent-Kafka producer instance.
        topic (str): The Kafka topic name for raw flight data (Bronze).
        bbox_info (dict): Bounding box coordinates (lat/min, lon/min, lat/max, lon/max).
    """
    def __init__(self, bootstrap_servers, topic, bbox_info):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic
        self.bbox_info = bbox_info

    def delivery_report(self, err, msg):
        """Callback triggered upon message acknowledgment from the Kafka broker.

        Args:
            err (KafkaError): The error object if delivery failed, else None.
            msg (Message): The Kafka message object containing metadata.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            pass

    def publish_to_bronze(self):
        """Fetches live flights and publishes each state vector to Kafka.

        This method executes the 'Extract' phase of the ingestion 
        pipeline. It converts raw flight lists into JSON bytes and utilizes 
        asynchronous production with a flush to ensure batch delivery.
        
        Note:
            The data is sent as raw lists to maintain a high-fidelity audit trail 
            in the Bronze layer.
        """
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