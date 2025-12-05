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
from producers.kafka_producer import KafkaProducer
from utils.config_loader import ConfigLoader
from utils.generator_util import generate_value
import time
import argparse
from threading import Thread, Lock
from queue import Queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



MESSAGE_COUNT = 1

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
    def __init__(self, kafka_broker: str, kafka_topic: str, *args, **kwargs):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.args = args
        self.kwargs = kwargs
        self.site = kwargs.get("site", "boomtrain")
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        self.faker = Faker()
        self.properties_schema = kwargs.get("properties", properties)
        self.total_messages = kwargs.get("total_messages", MESSAGE_COUNT)
        self.num_threads = kwargs.get("num_threads", 1)
        self.count_lock = Lock()
        self.start_time = None
        self.end_time = None
        self.flush_interval = kwargs.get("flush_interval", 100)  # Flush every N messages
        
        # Configuration for delta updates
        self.users_file = kwargs.get("users_file", "data/users_extracted.txt")
        self.num_fields_to_update = kwargs.get("num_fields_to_update", 2)
        self.users_file_handle = None
        self.users_file_lines = []
        self.use_file_input = False

        # Try to load users file
        if self.users_file and os.path.exists(self.users_file):
            try:
                # If file is not too large, read into memory for speed
                # Otherwise, we'd need a file pointer approach
                # For now, let's try reading lines. If it fails, we handle it.
                with open(self.users_file, 'r') as f:
                    self.users_file_lines = [line.strip() for line in f if line.strip()]
                
                if self.users_file_lines:
                    self.use_file_input = True
                    logger.info(f"Loaded {len(self.users_file_lines)} user records from {self.users_file}")
                else:
                    logger.warning(f"Users file {self.users_file} is empty")
            except Exception as e:
                logger.error(f"Failed to load users file: {e}")
        else:
            logger.warning(f"Users file {self.users_file} not found.")

        # Load BSINs from file if provided (fallback or auxiliary)
        self.bsins = []
        self.bsin_index = 0
        bsin_file = kwargs.get("bsin_file", None)
        if bsin_file and not self.use_file_input:
            self._load_bsins(bsin_file)
    
    def _load_bsins(self, file_path: str):
        """Load BSINs from a file"""
        try:
            with open(file_path, 'r') as f:
                self.bsins = [line.strip() for line in f if line.strip()]
            self.bsin_length = len(self.bsins)
            logger.info(f"Loaded {len(self.bsins)} BSINs from {file_path}")
        except Exception as e:
            logger.error(f"Failed to load BSINs from {file_path}: {e}")
            raise
    
    def _get_next_bsin(self):
        """Get the next BSIN from the loaded list (cycles through if needed)"""
        if not self.bsins:
            return self.faker.uuid4()
        
        return random.choice(self.bsins)

    def _get_random_user_record(self):
        """Get a random user record from loaded file lines"""
        if not self.users_file_lines:
            return None
        return random.choice(self.users_file_lines)

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
        if not all_paths:
            return []
        # Ensure we don't try to select more than available
        count = min(self.num_fields_to_update, len(all_paths))
        return random.sample(all_paths, count)

    def generate_user_props(self, base_record_str=None):
        """Generate user properties record based on the fields and datatypes"""
        
        # Handle Update Mode
        if self.use_file_input and base_record_str:
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
                
                return {"insert": user_props}
            except Exception as e:
                logger.error(f"Failed to update record: {e}")
                return None

        # Standard generation mode (fallback)
        # Generate base fields from DEFAULT_MESSAGE_FIELDS
        user_props = {}
        for field, field_type in DEFAULT_MESSAGE_FIELDS.items():
            if field == "properties":
                # Generate properties hash based on properties_schema
                props = {}
                for prop_name, prop_type in self.properties_schema.items():
                    props[prop_name] = self._generate_value(prop_name, prop_type)
                user_props[field] = props
            else:
                user_props[field] = self._generate_value(field, field_type)
        
        # Override site_id with configured site
        user_props["site_id"] = self.site
        
        # Add additional required fields with empty/default values
        user_props.update({
            "replaced_by": "",
            "email_md5": "",
            "sub_site_ids": [],
            "scoped_properties": {},
            "scoped_contacts": {self.site: user_props["contacts"]},
            "imported_from": {},
            "consent": {},
            "external_ids": {},
            "unique_client_ids": [],
            "merged_bsins": []
        })
        
        # Wrap in insert format
        return {"insert": user_props}

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
            self.push_to_kafka(user_props)
            
            local_count += 1
            # Periodic flush to avoid buffering
            if local_count % self.flush_interval == 0:
                self.kafka_producer.flush()
            
            message_queue.task_done()

    def run(self):
        self.start_time = time.time()
        logger.info(f"Starting message generation: Total={self.total_messages}, Threads={self.num_threads}")
        if self.use_file_input:
             logger.info(f"Mode: Delta Updates from file {self.users_file}")
             logger.info(f"Updating {self.num_fields_to_update} random fields per record")
        message_queue = Queue()

        if self.num_threads > 1:
            # Multi-threaded execution
            threads = []
            
            # Start worker threads
            for _ in range(self.num_threads):
                thread = Thread(target=self._worker_thread, args=(message_queue,))
                thread.start()
                threads.append(thread)
            
            # Add tasks to the queue
            count = 0
            for base_record in self.users_file_lines:
                if count >= self.total_messages:
                    break
                message_queue.put(base_record)
                count += 1
            
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
            
            for base_record in self.users_file_lines:
                if count >= self.total_messages:
                    break
                # message_queue.put(base_record)
                count += 1
                
                # Generate user properties
                user_props = self.generate_user_props(base_record_str=base_record)
                # Push to Kafka
                if user_props:
                    # print(user_props)
                    self.push_to_kafka(user_props)
                else:
                    continue
                # Periodic flush to avoid buffering
                if count % self.flush_interval == 0:
                    self.kafka_producer.flush()
        
        self.kafka_producer.flush()
        self.kafka_producer.close()
        
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        messages_per_sec = self.total_messages / elapsed_time if elapsed_time > 0 else 0
        
        # Report results
        print("\n" + "="*50)
        print("MESSAGE GENERATION REPORT")
        print("="*50)
        print(f"Total Messages Generated: {self.total_messages}")
        print(f"Time Taken: {elapsed_time:.2f} seconds")
        print(f"Throughput: {messages_per_sec:.2f} messages/sec")
        print(f"Threads Used: {self.num_threads}")
        print("="*50)


if __name__ == "__main__":
    # Load configuration
    config = ConfigLoader.load()
    
    parser = argparse.ArgumentParser(description='Generate user properties with random updates')
    parser.add_argument('--total-messages', type=int, default=6000000, help='Total number of messages to generate')
    parser.add_argument('--kafka-broker', type=str, default=None, help='Kafka broker address (overrides config file)')
    parser.add_argument('--kafka-topic', type=str, default=None, help='Kafka topic name (overrides config file)')
    parser.add_argument('--num_threads', type=int, default=None, help='Number of threads for parallel generation (overrides config file)')
    parser.add_argument('--bsin-file', type=str, default=None, help='Path to file containing BSINs (overrides config file)')
    parser.add_argument('--users-file', type=str, default=None, help='Path to file containing user records (overrides config file)')
    parser.add_argument('--num-fields', type=int, default=None, help='Number of fields to randomly update per record')
    parser.add_argument('--config', type=str, default=None, help='Path to config file (default: config.json)')
    
    args = parser.parse_args()
    
    # Reload config if custom config file specified
    if args.config:
        config = ConfigLoader.load(args.config)
    
    # Get configuration values with command-line overrides
    kafka_broker = args.kafka_broker or ConfigLoader.get_kafka_brokers()
    kafka_topic = args.kafka_topic or ConfigLoader.get_kafka_topic("identity")
    num_threads = args.num_threads or ConfigLoader.get_default("num_threads", 1)
    bsin_file = args.bsin_file or ConfigLoader.get_bsin_file()
    users_file = args.users_file or ConfigLoader.get("data.users_file", "data/users_extracted.txt")
    num_fields_to_update = args.num_fields or 2
    
    logger.info(f"Using Kafka Broker: {kafka_broker}")
    logger.info(f"Using Kafka Topic: {kafka_topic}")
    logger.info(f"Using {num_threads} threads")
    
    generator = UserPropsGeneratorNarrow(
        kafka_broker=kafka_broker,
        kafka_topic=kafka_topic,
        total_messages=args.total_messages,
        num_threads=num_threads,
        bsin_file=bsin_file,
        users_file=users_file,
        num_fields_to_update=num_fields_to_update
    )
    generator.run()
