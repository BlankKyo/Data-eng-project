# core/consumer.py
import json
import logging
from confluent_kafka import Consumer
from db.load import save_aircraft, save_flights_to_db, cleanup_old_data

# init logger
logger = logging.getLogger(__name__)

class FlightConsumer:
    """Consumes raw flight data from Kafka and persists it to the Silver Layer (DB).

    This class implements the 'Consumer' side of the Medallion Architecture. It 
    orchestrates the consumption of raw JSON arrays from Kafka, manages 
    micro-batching for optimized database I/O, and triggers periodic maintenance.

    Attributes:
        db_conn (connection): Active database connection 
        topic (str): The Kafka topic to subscribe to for ingestion.
        consumer (Consumer): Confluent-Kafka consumer instance.
    """
    def __init__(self, db_connection, bootstrap_servers, topic):
        """Initializes the consumer with database and broker configurations.

        Note: 
            The 'auto.offset.reset' is set to 'earliest' to ensure data 
            continuity in case of downtime or new consumer group deployment.
        """
        self.db_conn = db_connection
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers, 
            'group.id': 'aviation_group',
            'auto.offset.reset': 'earliest'
        })

    def process_messages(self, shutdown_event):
        """Main execution loop for polling and batching Kafka messages.

        This method continuously polls Kafka for new flight records. Once the 
        internal buffer reaches the 'batch_size' threshold, it triggers a 
        database flush. It handles JSON decoding and basic error recovery 
        to ensure the pipeline remains resilient to malformed messages.
        """
        self.consumer.subscribe([self.topic])
        batch = []
        batch_size = 10000
        
        logger.info(f"Consumer started. Listening to {self.topic}...")

        try:
            while not shutdown_event.is_set():
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    raw_flight = json.loads(msg.value().decode('utf-8'))
                    batch.append(raw_flight)
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
                    continue

                # batch processing
                if len(batch) >= batch_size:
                    self._flush_batch(batch)
                    batch.clear()

        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            shutdown_event.set()
        finally:
            self.consumer.close()

    def _flush_batch(self, batch):
        """Persists the current batch to the database and performs maintenance.

        This internal helper coordinates the atomic write operations for 
        aircraft metadata and flight positions. It also triggers data cleanup 
        to ensure the TimescaleDB storage footprint remains optimized.

        Args:
            batch (list): A list of raw flight state vectors extracted from Kafka.
        """
        try:
            logger.info(f"Processing batch of {len(batch)} flights...")
            
            save_aircraft(batch, self.db_conn)
            save_flights_to_db(batch, self.db_conn)
            
            # Maintenance
            cleanup_old_data(self.db_conn, hours=1)

            self.db_conn.commit()
            
            logger.info("Batch successfully saved to Silver Layer.")
        except Exception as e:
            logger.error(f"Database Load Error: {e}")
            self.db_conn.rollback()