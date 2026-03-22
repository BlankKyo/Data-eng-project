import logging
import os
import time
from core.extract import get_region_bbox
from db.database import get_connection
from core.producer import OpenSkyProducer
from core.consumer import FlightConsumer
import threading
import sys

# SETUP LOGGING
LOG_PATH = os.path.join("logs", "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# This flag tells threads when to stop
shutdown_event = threading.Event()

def run_producer_thread(server, topic, bbox_info):
    logger.info("Starting Producer Thread...")
    try:
        # Initialize your class
        p = OpenSkyProducer(server, topic, bbox_info)
        while not shutdown_event.is_set():
            p.publish_to_bronze()
            # Wait 30 seconds before next API pull to avoid being banned by OpenSky
            time.sleep(30) 
    except Exception as e:
        logger.error(f"Producer Thread CRASHED: {e}")
    finally:
        logger.info("Producer Thread cleaning up...")

def run_consumer_thread(conn, server, topic):
    logger.info("Starting Consumer Thread...")
    try:
        # Initialize your class
        c = FlightConsumer(conn, server, topic)
        # We pass the shutdown_event to the consumer logic
        c.process_messages()
    except Exception as e:
        logger.error(f"Consumer Thread CRASHED: {e}")
    finally:
        logger.info("Consumer Thread cleaning up...")

if __name__ == "__main__":
    KAFKA_SERVER = "kafka:29092"
    TOPIC = "flights_bronze"
    bbox_info = get_region_bbox("France")
    conn = get_connection() 
    # 2. Define Threads
    t1 = threading.Thread(target=run_producer_thread, args=(KAFKA_SERVER, TOPIC, bbox_info), name="ProducerWorker")
    t2 = threading.Thread(target=run_consumer_thread, args=(conn, KAFKA_SERVER, TOPIC), name="ConsumerWorker")

    try:
        t1.start()
        t2.start()

        # Keep the main thread alive while workers are running
        while t1.is_alive() or t2.is_alive():
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received (Ctrl+C)...")
        shutdown_event.set() # Tell threads to stop loops
        
        # Wait for threads to finish their current loop
        t1.join()
        t2.join()
        logger.info("All systems offline. Exit successful.")
        sys.exit(0)