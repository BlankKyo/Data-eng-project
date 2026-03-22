import json
import logging
from confluent_kafka import Consumer
from db.load import save_aircraft, save_flights_to_db, cleanup_old_data

# This automatically names the logger after the file
logger = logging.getLogger(__name__)

class FlightConsumer:
    def __init__(self, db_connection, bootstrap_servers, topic):
        self.db_conn = db_connection
        self.topic = topic
        # SDE PRO TIP: 'auto.offset.reset': 'earliest' ensures you don't miss data on first run
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers, 
            'group.id': 'aviation_group',
            'auto.offset.reset': 'earliest'
        })

    def process_messages(self):
        self.consumer.subscribe([self.topic])
        batch = []
        batch_size = 10000
        
        logger.info(f"Consumer started. Listening to {self.topic}...")

        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                # 1. Decode and Parse the JSON list
                try:
                    # 'msg.value()' is bytes, we turn it into a Python List
                    raw_flight = json.loads(msg.value().decode('utf-8'))
                    batch.append(raw_flight)
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
                    continue

                # 2. Batch Processing (The Silver Layer)
                if len(batch) >= batch_size:
                    self._flush_batch(batch)
                    batch.clear()

        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            self.consumer.close()

    def _flush_batch(self, batch):
        """Internal helper to save data and cleanup."""
        try:
            logger.info(f"Processing batch of {len(batch)} flights...")
            
            # --- IMPORTANT: Pass the WHOLE batch, not just one record ---
            save_aircraft(batch, self.db_conn)
            save_flights_to_db(batch, self.db_conn)
            
            # Maintenance
            cleanup_old_data(self.db_conn, hours=1)

            self.db_conn.commit()
            
            logger.info("Batch successfully saved to Silver Layer.")
        except Exception as e:
            logger.error(f"Database Load Error: {e}")
            # In production, you might want to 'Rollback' the DB here