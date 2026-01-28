# This script generates synthetic data for user properties and pushes to kafka topic.
# Inputs: Custom Fields with datatype n range selection for fields like dates, numbers, strings, booleans, etc. -- pushes to kafka messages in range/out of range
# Output: how many messages in range/out of range were pushed to kafka
# Sample messages format:
# {"insert": {"site_id": "boomtrain","bsin": "0002ac00-83ce-4071-bcd2-7a1089812b621","user_id": "6900077","email": "","contacts": {},"properties": {  "has_active_email": "true","all_emails": "true", "double_optin": "true", "shopping_cart":{"total": 5000},"signed_up_at": "2025-10-15 23:05:00", "appointment_time": "2025-10-15 05:31:00", "last_purchase": {"datafields": {"total":200, "items": [{"item_id": 1, "item_name": "brush"}]}} },"last_updated": "2025-04-25 0:00:00","replaced_by": "","email_md5": "","sub_site_ids": [],"scoped_properties": {},"scoped_contacts": {},"imported_from": {},"consent": {},"external_ids": {},"unique_client_ids": [],"merged_bsins": []   }}
# {"insert": {"site_id": "boomtrain","bsin": "6test2fecc6aa9-81cc-4370-aba9-a35d95ae4969","user_id": "6900077","email": "","contacts": {"email::hlingam@zetaglobal.com":  { "type": "email","value": "hlingam@zetaglobal.com","status": "inactive","preferences": ["standard"],"inactivity_reason": "unsubscribed","created_at": 1536238279,"updated_at": 1701117523,"last_inactivity_updated_at": 1605806750,"last_clicked": null,"last_opened": null,"last_sent": 1701117491,"last_purchased": null,"domain": "zetaglobal.com","signed_up_at": 1536238279,"double_opt_in_status": null,"country_code": null,"area_code": null,"timezone": null,"geolocation": null,"phone_type": null,"contact_properties": null}}, "properties": { "has_active_email": "true", "shopping_cart":{"total": 10000},"signed_up_at": "2025-10-21 21:00:00","signed_up": "2025-10-21 21:00:00", "appointment_time": "2025-10-15 05:31:00"  },"last_updated": "2025-04-25 0:00:00","replaced_by": "","email_md5": "","sub_site_ids": [],"scoped_properties": {},"scoped_contacts":{},"imported_from": {},"consent": {},"external_ids": {},"unique_client_ids": [],"merged_bsins": []   }}

import json
import logging
import random
import os
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaConsumer
from producers.kafka_producer import KafkaProducer
from utils.config_loader import ConfigLoader
from utils.generator_util import generate_value
import time
import argparse
from threading import Thread, Lock
from queue import Queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DEFAULT_MESSAGE_FIELDS = {
    "site_id": "string",
    "bsin": "uuid",
    "user_id": "uuid",
    "email": "email",
    "last_updated": "datetime",
    "contacts": "map<Contact>",
    "properties": {}
  }
  #   "replaced_by", "email_md5", "sub_site_ids", "scoped_properties", "scoped_contacts", "imported_from", "consent", "external_ids", "unique_client_ids"}

properties = {"has_active_email": "bool",
              "all_emails": "bool",
              "double_optin": "bool",
              "signed_up_at": "datetime",
              "appointment_time": "datetime",
              "last_purchase": "custom_struct:purchase"}

class UserPropsGeneratorNarrow:
    def __init__(self, kafka_broker: str, kafka_topic: str, source_kafka_topic: str, *args, **kwargs):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.source_kafka_topic = source_kafka_topic
        self.args = args
        self.kwargs = kwargs
        self.site = kwargs.get("site", "boomtrain")
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        self.faker = Faker()
        self.properties_schema = kwargs.get("properties", properties)
        self.num_threads = kwargs.get("num_threads", 1)
        self.count_lock = Lock()
        self.start_time = None
        self.end_time = None
        self.flush_interval = kwargs.get("flush_interval", 100)  # Flush every N messages
        self.msg_format = kwargs.get("msg_format", "raw")  # "raw" or "insert_delete"
        
        self.num_fields_to_update = kwargs.get("num_fields_to_update", 2)
        
        # Initialize Kafka Consumer
        try:
            self.consumer = KafkaConsumer(
                self.source_kafka_topic,
                bootstrap_servers=self.kafka_broker,
                auto_offset_reset='earliest',
                group_id=f'user_props_generator_group_{int(time.time())}',  # Unique group ID to force consuming from start
                value_deserializer=lambda x: x.decode('utf-8'),
                # consumer_timeout_ms=10000 # REMOVED to allow indefinite loop
            )
            logger.info(f"Initialized Kafka Consumer for source topic: {self.source_kafka_topic}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Consumer: {e}")
            raise

        # BSIN list is empty as we don't load from file
        self.bsins = []

    def _get_next_bsin(self):
        """Get the next BSIN from the loaded list (cycles through if needed)"""
        # Since we removed file loading, always return random
        return self.faker.uuid4()

    def _generate_value(self, field_name: str, data_type: str):
        """Generate mock value based on data type"""
        return generate_value(self.faker, field_name, data_type, self._get_next_bsin, None)

    def _update_nested_value(self, data, field_path, new_value):
        """Update a value in a nested dictionary using dot notation"""
        keys = field_path.split('.')
        current = data
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            elif not isinstance(current[key], dict):
                # If we hit a non-dict, we can't descend further. 
                # Depending on use case, we might want to overwrite or skip.
                # Here we skip to avoid breaking structure unless necessary.
                return
            current = current[key]
        current[keys[-1]] = new_value

    def _get_field_type(self, field_path):
        """Infer field type from schema or name"""
        if field_path.startswith("properties."):
            prop_name = field_path.split(".", 1)[1]
            if prop_name in self.properties_schema:
                return self.properties_schema[prop_name]
        
        # Fallback based on name
        if "email" in field_path: return "email"
        if "_at" in field_path or "time" in field_path: return "datetime"
        if "bool" in field_path or "is_" in field_path or "has_" in field_path: return "bool"
        return "string"

    def _get_all_property_paths(self):
        """Flatten properties schema into a list of dot-notation paths"""
        paths = []
        for prop, dtype in self.properties_schema.items():
            if dtype.startswith("custom_struct:"):
                pass
                # For now, we don't fully expand custom structs dynamically without definition,
                # but assuming last_purchase structure based on typical usage
                # if prop == "last_purchase":
                #    # Hardcoded sub-fields for last_purchase based on sample message
                #    # "last_purchase": {"datafields": {"total":200, "items": [{"item_id": 1, "item_name": "brush"}]}}
                #    # We can update total, or maybe we just treat last_purchase as a whole update?
                #    # Let's expose some leaf nodes
                #    paths.append(f"properties.{prop}.datafields.total")
            else:
                paths.append(f"properties.{prop}")
        return paths

    def _get_random_fields_to_update(self):
        """Select N random fields to update"""
        all_paths = self._get_all_property_paths()
        return random.sample(all_paths, self.num_fields_to_update)

    def generate_user_props(self, base_record_str=None):
        """Generate user properties record based on the fields and datatypes"""
        
        # We expect base_record_str to be present from Kafka source
        if base_record_str:
            try:
                record_json = json.loads(base_record_str)
                # Handle wrapped (insert) or unwrapped
                user_props = record_json.get("insert", record_json)
                
                # Update timestamp
                user_props["last_updated"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Select random fields to update for this record
                fields_to_update = self._get_random_fields_to_update()
                
                for field_path in fields_to_update:
                    # Infer simple name for condition checking (last part)
                    field_name = field_path.split('.')[-1]
                    
                    # Determine field type based on path or name
                    field_type = "string" # default
                    if "total" in field_path: field_type = "int"
                    else: field_type = self._get_field_type(field_path)
                    
                    new_value = self._generate_value(field_name, field_type)
                    self._update_nested_value(user_props, field_path, new_value)
                
                # Wrap in insert format based on msg_format config
                if self.msg_format == "insert_delete":
                    return {"insert": user_props}
                else:
                    # Return raw data directly
                    return user_props
            except Exception as e:
                logger.error(f"Failed to update record: {e}")
                return None
        
        return None

    def push_to_kafka(self, user_props: dict):
        try:
            # print(user_props)
            self.kafka_producer.send(self.kafka_topic, json.dumps(user_props).encode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise

    def _worker_thread(self, message_queue: Queue):
        """Worker thread to generate and push messages"""
        local_count = 0
        
        while True:
            task = message_queue.get()
            if task is None:  # Poison pill to stop the thread
                message_queue.task_done()
                break
            
            base_record_str = task
            
            # Generate user properties
            user_props = self.generate_user_props(base_record_str=base_record_str)
            
            if not user_props:
                message_queue.task_done()
                continue
            
            # Push to Kafka
            # print(user_props)
            self.push_to_kafka(user_props)
            
            local_count += 1
            # Periodic flush to avoid buffering
            if local_count % self.flush_interval == 0:
                self.kafka_producer.flush()
            
            message_queue.task_done()

    def run(self):
        self.start_time = time.time()
        logger.info(f"Starting message generation: Threads={self.num_threads}")
        logger.info(f"Mode: Delta Updates from Source Kafka Topic {self.source_kafka_topic}")
        logger.info(f"Updating {self.num_fields_to_update} random fields per record")
        logger.info("Starting continuous consumption loop... (Press Ctrl+C to stop)")
        
        message_queue = Queue(maxsize=1000) # Bounded queue

        if self.num_threads > 1:
            # Multi-threaded execution
            threads = []
            
            # Start worker threads
            for _ in range(self.num_threads):
                thread = Thread(target=self._worker_thread, args=(message_queue,))
                thread.start()
                threads.append(thread)
            
            # Consume and Add tasks to the queue
            count = 0
            try:
                for message in self.consumer:
                    message_queue.put(message.value)
                    count += 1
                    if count % 1000 == 0:
                        logger.info(f"Queued {count} messages from Kafka")
            except KeyboardInterrupt:
                logger.info("Received shutdown signal (Ctrl+C)")
            except Exception as e:
                logger.error(f"Error during consumption: {e}")
            
            logger.info(f"Consumption stopped. Total messages queued: {count}")
            
            # Add poison pills to stop threads
            for _ in range(self.num_threads):
                message_queue.put(None)
            
            # Wait for all tasks to complete
            message_queue.join()
            
            # Wait for all threads to finish
            for thread in threads:
                thread.join()
        else:
            # Single-threaded execution
            count = 0
            logger.info("Starting consumption from Kafka source (Single Threaded)...")
            try:
                for message in self.consumer:
                    
                    user_props = self.generate_user_props(base_record_str=message.value)
                    if user_props:
                        self.push_to_kafka(user_props)
                    
                    count += 1
                    if count % self.flush_interval == 0:
                        self.kafka_producer.flush()
                        logger.info(f"Processed {count} messages")
            except KeyboardInterrupt:
                logger.info("Received shutdown signal (Ctrl+C)")
            except Exception as e:
                logger.error(f"Error during consumption: {e}")
        
        self.kafka_producer.flush()
        self.kafka_producer.close()
        self.consumer.close()
        
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        messages_per_sec = count / elapsed_time if elapsed_time > 0 else 0
        
        # Report results
        print("\n" + "="*50)
        print("MESSAGE GENERATION REPORT")
        print("="*50)
        print(f"Total Messages Generated: {count}")
        print(f"Time Taken: {elapsed_time:.2f} seconds")
        print(f"Throughput: {messages_per_sec:.2f} messages/sec")
        print(f"Threads Used: {self.num_threads}")
        print("="*50)


if __name__ == "__main__":
    # Load configuration
    config = ConfigLoader.load()
    
    parser = argparse.ArgumentParser(description='Generate user properties with random updates')
    parser.add_argument('--kafka-broker', type=str, default=None, help='Kafka broker address (overrides config file)')
    parser.add_argument('--kafka-topic', type=str, default=None, help='Destination Kafka topic name (overrides config file)')
    parser.add_argument('--source-topic', type=str, default=None, help='Source Kafka topic name (REQUIRED)')
    parser.add_argument('--num_threads', type=int, default=None, help='Number of threads for parallel generation (overrides config file)')
    parser.add_argument('--num-fields', type=int, default=None, help='Number of fields to randomly update per record')
    parser.add_argument('--config', type=str, default=None, help='Path to config file (default: config.json)')
    parser.add_argument('--msg-format', type=str, default='raw', choices=['raw', 'insert_delete'], help='Message format: raw (default) or insert_delete')
    
    args = parser.parse_args()
    
    # Reload config if custom config file specified
    if args.config:
        config = ConfigLoader.load(args.config)
    
    # Get configuration values with command-line overrides
    kafka_broker = args.kafka_broker or ConfigLoader.get_kafka_brokers()
    kafka_topic = args.kafka_topic or ConfigLoader.get_kafka_topic("identity_delta")
    source_kafka_topic = args.source_topic or ConfigLoader.get_kafka_topic("identity")
    num_threads = args.num_threads or ConfigLoader.get_default("num_threads", 1)
    num_fields_to_update = args.num_fields or 2
    
    logger.info(f"Using Kafka Broker: {kafka_broker}")
    logger.info(f"Destination Topic: {kafka_topic}")
    logger.info(f"Source Topic: {source_kafka_topic}")
    logger.info(f"Using {num_threads} threads")
    logger.info(f"Using message format: {args.msg_format}")
    
    generator = UserPropsGeneratorNarrow(
        kafka_broker=kafka_broker,
        kafka_topic=kafka_topic,
        source_kafka_topic=source_kafka_topic,
        num_threads=num_threads,
        num_fields_to_update=num_fields_to_update,
        msg_format=args.msg_format
    )
    generator.run()
