import json
import logging
import argparse
import os
import sys
import signal
import uuid
import io
import time
import fastavro
from kafka import KafkaConsumer

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.config_loader import ConfigLoader
from producers.kafka_producer import KafkaProducer
# Import schema generator
from utils.user_schema import generate_avro_schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UserPropsFlattenConsumer:
    """
    Consumer that reads Avro messages with an "insert" wrapper from an input topic,
    removes the wrapper, generates a new BSIN, and produces the flattened Avro message
    to an output topic.
    """
    def __init__(self, bootstrap_servers, input_topic, output_topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id
        self.running = True
        self.total_processed = 0
        
        # Initialize schema
        # We generate the schema directly from the utility function
        try:
            self.full_schema = generate_avro_schema()
            self.parsed_full_schema = fastavro.parse_schema(self.full_schema)
        except Exception as e:
            logger.error(f"Failed to generate schema: {e}")
            raise
        
        # Extract the inner schema (UserProps)
        # Structure is {type: record, fields: [{name: insert, type: INNER_SCHEMA}]}
        # We assume the schema structure matches what is in user_props_generator.py
        try:
            insert_field = next(f for f in self.full_schema['fields'] if f['name'] == 'insert')
            self.inner_schema = insert_field['type']
            self.parsed_inner_schema = fastavro.parse_schema(self.inner_schema)
        except StopIteration:
            logger.error("Could not find 'insert' field in schema")
            raise ValueError("Schema does not contain 'insert' field")
        except Exception as e:
            logger.error(f"Error parsing inner schema: {e}")
            raise
        
        logger.info("Schemas initialized successfully")

        # Initialize Consumer
        try:
            self.consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=None  # We want raw bytes
            )
            logger.info(f"Consumer initialized for topic: {self.input_topic}")
        except Exception as e:
            logger.error(f"Failed to initialize consumer: {e}")
            raise

        # Initialize Producer
        try:
            self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
            logger.info(f"Producer initialized for topic: {self.output_topic}")
        except Exception as e:
            logger.error(f"Failed to initialize producer: {e}")
            raise
            
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("Received shutdown signal")
        self.running = False

    def run(self):
        logger.info(f"Starting consumption loop from {self.input_topic} -> {self.output_topic}")
        
        while self.running:
            try:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    continue

                for partition, messages in message_batch.items():
                    for message in messages:
                        if not self.running:
                            break
                            
                        self._process_message(message)
                
                # Periodically flush producer
                # self.producer.flush() # Flushing too often might hurt performance, relying on producer batching
                        
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                # Don't crash the loop on transient errors
                
        self.close()

    def _process_message(self, message):
        try:
            value_bytes = message.value
            if not value_bytes:
                return

            # Decode using full schema (schemaless)
            bytes_io = io.BytesIO(value_bytes)
            try:
                record = fastavro.schemaless_reader(bytes_io, self.parsed_full_schema)
            except Exception as e:
                logger.error(f"Failed to decode Avro message at offset {message.offset}: {e}")
                return
            
            if 'insert' in record:
                user_props = record['insert']
                for i in range(200):
                # Generate new BSIN
                    user_props['bsin'] = str(uuid.uuid4())
                    
                    # Serialize using inner schema
                    out_bytes = io.BytesIO()
                    fastavro.schemaless_writer(out_bytes, self.parsed_inner_schema, user_props)
                    
                    # Send to output topic
                    # print(user_props)
                    self.producer.send(self.output_topic, out_bytes.getvalue())
                    
                    self.total_processed += 1
                    if self.total_processed % 1000 == 0:
                        logger.info(f"Processed {self.total_processed} records")

        except Exception as e:
            logger.error(f"Error processing message at offset {message.offset}: {e}")

    def close(self):
        logger.info("Closing resources...")
        if hasattr(self, 'producer'):
            self.producer.flush()
            self.producer.close()
        if hasattr(self, 'consumer'):
            self.consumer.close()
        logger.info(f"Finished. Total processed: {self.total_processed}")

if __name__ == "__main__":
    # Load config defaults
    ConfigLoader.load()
    default_brokers = ConfigLoader.get_kafka_brokers()

    parser = argparse.ArgumentParser(description='Flatten UserProps Avro messages and re-key BSIN')
    parser.add_argument('--bootstrap-servers', type=str, default=default_brokers, help=f'Kafka bootstrap servers (default: {default_brokers})')
    parser.add_argument('--input-topic', type=str, default="seg_poc_identity_wide_avro", help='Source topic with "insert" wrapper')
    parser.add_argument('--output-topic', type=str, default="seg_poc_identity_wide_avro1", help='Destination topic for flattened records')
    parser.add_argument('--group-id', type=str, default=f'user_props_flatten_group_{int(time.time())}')
    
    args = parser.parse_args()
    
    logger.info(f"Configuration: Brokers={args.bootstrap_servers}, Input={args.input_topic}, Output={args.output_topic}")

    consumer = UserPropsFlattenConsumer(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        group_id=args.group_id
    )
    consumer.run()

