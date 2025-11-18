# This script generates synthetic data for user events and pushes to kafka topic.
# Formats of the events: 
# {"site_id":"boomtrain","bsin":"6test2fecc6aa9-81cc-4370-aba9-a35d95ae4969","event_id":"73dabf77-b64c-48d6-bc-a59ac08f65a4","event_type":"purchased","identified":"","resource_id":"","resource_type":"","dt":"2025-10-21 00:56:14","url":"","identity":{"bsin":"9722f9ff-d9d8-4ece-8468-c0dc5533a066","user_id":"","email":"rdey@zetaglobal.com"},"metadata":{"receive_id":"e1134de0-c898-4844-a8d2-c5f0591842d6","receive_hostname":"kt-event-service-a","receive_timestamp":"2025-02-25T04:06:14.074594+00:00","process_id":"44b93ecc-b739-4e14-adb7-15104a309698","process_hostname":"event-service-processor-h","process_timestamp":"2025-02-25T04:06:14.089356+00:00"},"properties":{"cart_total":3000,"departure_time":"2025-10-21 00:38:13","nudge_token": "2390887eb3dd297f4cfe3f1ff53ae448","bsin":"9722f9ff-d9d8-4ece-8468-c0dc5533a066","value":"","expr_id":"boomtrain::journey::4464ff65ca6350d6cf300c07e9854f50::1647448199621::2","timestamp":"2025-02-25T04:06:14.073Z"},"property_format":"JSON", "status":"PROCESSED"}

import json
import logging
import random
from datetime import datetime, timedelta
from faker import Faker
from producers.kafka_producer import KafkaProducer
import time
import argparse
from threading import Thread, Lock
from queue import Queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MESSAGE_COUNT = 1
CONDITIONAL_FIELDS = []
# CONDITIONAL_FIELDS = ["and", ["gt", "event.properties.cart_total", "gt", "1000"], ["eq", "event.event_type", "purchased"]]

DEFAULT_EVENT_FIELDS = {
    "site_id": "string",
    "bsin": "uuid",
    "event_id": "uuid",
    "event_type": "string",
    "identified": "string",
    "resource_id": "string",
    "resource_type": "string",
    "dt": "datetime",
    "url": "string",
    "identity": {"bsin": "uuid", "user_id": "string", "email": "email"},
    "metadata": {
        "receive_id": "uuid",
        "receive_hostname": "string",
        "receive_timestamp": "iso_datetime",
        "process_id": "uuid",
        "process_hostname": "string",
        "process_timestamp": "iso_datetime"
    },
    "properties": {},
    "property_format": "string",
    "status": "string"
}

DEFAULT_EVENT_NAMES = ["purchase", "viewed", "clicked", "added_to_cart", "abandoned_cart"]

DEFAULT_EVENT_PROPERTIES = {
    "purchase": {
        "cart_total": "int",
        "departure_time": "datetime",
        "nudge_token": "string",
        "timestamp": "iso_datetime",
        "value": "int"
    },
    "viewed": {
        "nudge_token": "string",
        "timestamp": "iso_datetime",
        "resource_type": "string"
    },
    "clicked": {
        "nudge_token": "string",
        "timestamp": "iso_datetime",
        "url": "string"
    },
    "added_to_cart": {
        "cart_total": "int",
        "nudge_token": "string",
        "timestamp": "iso_datetime"
    },
    "abandoned_cart": {
        "cart_total": "int",
        "departure_time": "datetime",
        "nudge_token": "string",
        "timestamp": "iso_datetime"
    }
}


class EventsGenerator:
    def __init__(self, kafka_broker: str, kafka_topic: str, *args, **kwargs):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.args = args
        self.kwargs = kwargs
        self.site = kwargs.get("site", "boomtrain")
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        self.faker = Faker()
        self.event_names = kwargs.get("event_names", DEFAULT_EVENT_NAMES)
        self.properties_schema = kwargs.get("properties", DEFAULT_EVENT_PROPERTIES)
        self.conditional_fields = kwargs.get("conditional_fields", CONDITIONAL_FIELDS)
        self.total_messages = kwargs.get("total_messages", MESSAGE_COUNT)
        self.target_true_count = kwargs.get("target_true_count", self.total_messages//2)
        self.num_threads = kwargs.get("num_threads", 1)
        self.true_count = 0
        self.false_count = 0
        self.count_lock = Lock()
        self.start_time = None
        self.end_time = None
        self.flush_interval = kwargs.get("flush_interval", 1000)  # Flush every N messages

    def _parse_conditions(self):
        """Parse conditional fields to extract field requirements"""
        conditions = []
        if not self.conditional_fields:
            return conditions
        
        def parse_condition(cond):
            if isinstance(cond, list) and len(cond) >= 2:
                operator = cond[0]
                if operator == "and" or operator == "or":
                    # Recursively parse sub-conditions
                    return [parse_condition(c) for c in cond[1:]]
                elif operator in ["eq", "neq"]:
                    # Extract field path and expected value
                    field_path = cond[1].replace("event.", "")
                    expected_value = cond[2]
                    return {"type": operator, "field": field_path, "value": expected_value}
                elif operator in ["gt", "lt", "gte", "lte"]:
                    # Extract field path and comparison value
                    field_path = cond[1].replace("event.", "")
                    expected_value = cond[2] if len(cond) > 2 else None
                    return {"type": operator, "field": field_path, "value": expected_value}
                elif operator in ["withinlast", "withinnext"]:
                    # Extract field path and time range
                    field_path = cond[1].replace("event.", "")
                    days = int(cond[2])
                    return {"type": operator, "field": field_path, "days": days}
            return None
        
        return parse_condition(self.conditional_fields)

    def _generate_value(self, field_name: str, data_type: str, condition_value=None):
        """Generate mock value based on data type"""
        if condition_value is not None:
            return condition_value
        
        if data_type == "string":
            if field_name == "event_type":
                return random.choice(self.event_names)
            elif field_name == "property_format":
                return "JSON"
            elif field_name == "status":
                return random.choice(["PROCESSED", "PENDING", "FAILED"])
            elif field_name.endswith("hostname"):
                return self.faker.hostname()
            return self.faker.word()
        elif data_type == "uuid":
            return self.faker.uuid4()
        elif data_type == "email":
            return self.faker.email()
        elif data_type == "datetime":
            return self.faker.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
        elif data_type == "iso_datetime":
            return self.faker.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+00:00'
        elif data_type == "int":
            return random.randint(100, 10000)
        elif data_type == "bool_str":
            return str(random.choice([True, False])).lower()
        elif isinstance(data_type, dict):
            # Handle nested objects
            result = {}
            for key, value_type in data_type.items():
                result[key] = self._generate_value(key, value_type)
            return result
        else:
            return ""

    def _evaluate_condition(self, event_data, condition):
        """Evaluate if a condition is true for the given event data"""
        if isinstance(condition, list):
            operator = condition[0] if condition else None
            if operator == "and":
                return all(self._evaluate_condition(event_data, c) for c in condition[1:])
            elif operator == "or":
                return any(self._evaluate_condition(event_data, c) for c in condition[1:])
        elif isinstance(condition, dict):
            cond_type = condition.get("type")
            field_path = condition.get("field")
            
            # Extract value from nested path
            value = event_data
            for key in field_path.split("."):
                value = value.get(key) if isinstance(value, dict) else None
                if value is None:
                    return False
            
            if cond_type == "eq":
                return str(value) == str(condition.get("value"))
            elif cond_type == "neq":
                return str(value) != str(condition.get("value"))
            elif cond_type in ["gt", "lt", "gte", "lte"]:
                try:
                    val = float(value) if isinstance(value, (int, float, str)) else 0
                    comp_val = float(condition.get("value", 0))
                    if cond_type == "gt":
                        return val > comp_val
                    elif cond_type == "lt":
                        return val < comp_val
                    elif cond_type == "gte":
                        return val >= comp_val
                    elif cond_type == "lte":
                        return val <= comp_val
                except:
                    return False
            elif cond_type == "withinlast":
                try:
                    if 'T' in str(value):
                        dt = datetime.strptime(value.split('+')[0], '%Y-%m-%dT%H:%M:%S.%f')
                    else:
                        dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    days_ago = datetime.now() - timedelta(days=condition.get("days"))
                    return dt >= days_ago
                except:
                    return False
        return False

    def generate_event(self, should_match_condition=False):
        """Generate event record based on the fields and datatypes"""
        parsed_conditions = self._parse_conditions()
        
        # First, determine the event_type
        event_type = None
        if should_match_condition and parsed_conditions:
            event_type = self._get_condition_value("event_type", parsed_conditions, True)
        elif not should_match_condition and parsed_conditions:
            event_type = self._get_condition_value("event_type", parsed_conditions, False)
        
        if not event_type:
            event_type = random.choice(self.event_names)
        
        # Get properties schema for this event type
        if isinstance(self.properties_schema, dict) and event_type in self.properties_schema:
            event_properties_schema = self.properties_schema[event_type]
        elif isinstance(self.properties_schema, dict) and any(isinstance(v, dict) for v in self.properties_schema.values()):
            # properties_schema is event-name-based, but event_type not found, use first available
            event_properties_schema = next((v for v in self.properties_schema.values() if isinstance(v, dict)), {})
        else:
            # properties_schema is flat (backward compatibility)
            event_properties_schema = self.properties_schema
        
        # Generate base event fields
        event_data = {}
        for field, field_type in DEFAULT_EVENT_FIELDS.items():
            if field == "properties":
                # Generate properties based on event-specific properties_schema
                props = {}
                for prop_name, prop_type in event_properties_schema.items():
                    condition_value = None
                    
                    if should_match_condition and parsed_conditions:
                        condition_value = self._get_condition_value(prop_name, parsed_conditions, True)
                    elif not should_match_condition and parsed_conditions:
                        condition_value = self._get_condition_value(prop_name, parsed_conditions, False)
                    
                    props[prop_name] = self._generate_value(prop_name, prop_type, condition_value)
                event_data[field] = props
            elif field == "event_type":
                # Use the already determined event_type
                event_data[field] = event_type
            else:
                condition_value = None
                if should_match_condition and parsed_conditions:
                    condition_value = self._get_condition_value(field, parsed_conditions, True)
                elif not should_match_condition and parsed_conditions:
                    condition_value = self._get_condition_value(field, parsed_conditions, False)
                
                event_data[field] = self._generate_value(field, field_type, condition_value)
        
        # Override site_id with configured site
        event_data["site_id"] = self.site
        
        return event_data

    def _get_condition_value(self, field_name, conditions, should_match):
        """Extract the value needed for a field based on conditions"""
        def extract_value(cond):
            if isinstance(cond, list):
                for c in cond:
                    val = extract_value(c)
                    if val is not None:
                        return val
            elif isinstance(cond, dict):
                field_path = cond.get("field", "")
                if field_path.endswith(field_name) or field_path.split(".")[-1] == field_name:
                    if cond["type"] == "eq":
                        return cond["value"] if should_match else ("other_value" if isinstance(cond["value"], str) else 0)
                    elif cond["type"] in ["gt", "lt", "gte", "lte"]:
                        try:
                            comp_val = int(cond["value"])
                            if should_match:
                                if cond["type"] == "gt":
                                    return random.randint(comp_val + 1, comp_val + 5000)
                                elif cond["type"] == "lt":
                                    return random.randint(0, comp_val - 1)
                                elif cond["type"] == "gte":
                                    return random.randint(comp_val, comp_val + 5000)
                                elif cond["type"] == "lte":
                                    return random.randint(0, comp_val)
                            else:
                                if cond["type"] == "gt":
                                    return random.randint(0, comp_val)
                                elif cond["type"] == "lt":
                                    return random.randint(comp_val, comp_val + 5000)
                                elif cond["type"] == "gte":
                                    return random.randint(0, comp_val - 1)
                                elif cond["type"] == "lte":
                                    return random.randint(comp_val + 1, comp_val + 5000)
                        except:
                            pass
                    elif cond["type"] == "withinlast":
                        if should_match:
                            days = cond["days"]
                            dt = self.faker.date_time_between(start_date=f'-{days}d', end_date='now')
                            return dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            days = cond["days"]
                            dt = self.faker.date_time_between(start_date='-2y', end_date=f'-{days+1}d')
                            return dt.strftime('%Y-%m-%d %H:%M:%S')
            return None
        
        return extract_value(conditions)

    def push_to_kafka(self, event_data: dict):
        try:
            print(event_data)
            # self.kafka_producer.send(self.kafka_topic, json.dumps(event_data).encode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise

    def _worker_thread(self, message_queue: Queue):
        """Worker thread to generate and push events"""
        parsed_conditions = self._parse_conditions()
        local_count = 0
        
        while True:
            task = message_queue.get()
            if task is None:  # Poison pill to stop the thread
                message_queue.task_done()
                break
            
            should_match = task
            
            # Generate event
            event_data = self.generate_event(should_match_condition=should_match)
            
            # Verify the condition evaluation (for tracking)
            condition_met = self._evaluate_condition(event_data, parsed_conditions) if parsed_conditions else False
            
            with self.count_lock:
                if condition_met:
                    self.true_count += 1
                else:
                    self.false_count += 1
            
            # Push to Kafka
            self.push_to_kafka(event_data)
            
            local_count += 1
            # Periodic flush to avoid buffering
            if local_count % self.flush_interval == 0:
                self.kafka_producer.flush()
            
            message_queue.task_done()

    def run(self):
        self.start_time = time.time()
        logger.info(f"Starting event generation: Total={self.total_messages}, Target True={self.target_true_count}, Threads={self.num_threads}")
        
        if self.num_threads > 1:
            # Multi-threaded execution
            message_queue = Queue()
            threads = []
            
            # Start worker threads
            for _ in range(self.num_threads):
                thread = Thread(target=self._worker_thread, args=(message_queue,))
                thread.start()
                threads.append(thread)
            
            # Add tasks to the queue
            true_count_target = self.target_true_count
            for i in range(self.total_messages):
                should_match = i < true_count_target
                message_queue.put(should_match)
            
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
            parsed_conditions = self._parse_conditions()
            
            while count < self.total_messages:
                should_match = self.true_count < self.target_true_count
                
                event_data = self.generate_event(should_match_condition=should_match)
                
                condition_met = self._evaluate_condition(event_data, parsed_conditions) if parsed_conditions else False
                
                if condition_met:
                    self.true_count += 1
                else:
                    self.false_count += 1
                
                self.push_to_kafka(event_data)
                count += 1
                
                # Periodic flush to avoid buffering
                if count % self.flush_interval == 0:
                    self.kafka_producer.flush()
        
        self.kafka_producer.flush()
        self.kafka_producer.close()
        
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        messages_per_sec = self.total_messages / elapsed_time if elapsed_time > 0 else 0
        
        print("\n" + "="*50)
        print("EVENT GENERATION REPORT")
        print("="*50)
        print(f"Total Events Generated: {self.total_messages}")
        print(f"Events with Condition TRUE: {self.true_count}")
        print(f"Events with Condition FALSE: {self.false_count}")
        print(f"Target True Count: {self.target_true_count}")
        print(f"Actual Match Rate: {(self.true_count/self.total_messages)*100:.2f}%")
        print(f"Time Taken: {elapsed_time:.2f} seconds")
        print(f"Throughput: {messages_per_sec:.2f} messages/sec")
        print(f"Threads Used: {self.num_threads}")
        print("="*50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate events with conditional fields')
    parser.add_argument('--total-messages', type=int, default=100, help='Total number of events to generate')
    parser.add_argument('--target-true-count', type=int, default=50, help='Target number of events where condition is true')
    parser.add_argument('--kafka-broker', type=str, default='localhost:19092', help='Kafka broker address')
    parser.add_argument('--kafka-topic', type=str, default='user_events', help='Kafka topic name')
    parser.add_argument('--site', type=str, default='boomtrain', help='Site ID for events')
    parser.add_argument('--event-names', type=str, default=None, help='Comma-separated list of event names')
    parser.add_argument('--conditional-fields', type=str, default=None, help='JSON string of conditional fields')
    parser.add_argument('--properties', type=str, default=None, help='JSON string of event properties schema')
    parser.add_argument('--num-threads', type=int, default=1, help='Number of threads for parallel generation')
    
    args = parser.parse_args()
    
    # Parse event names if provided
    event_names = DEFAULT_EVENT_NAMES
    if args.event_names:
        event_names = [name.strip() for name in args.event_names.split(',')]
    
    # Parse conditional fields if provided
    conditional_fields = CONDITIONAL_FIELDS
    if args.conditional_fields:
        try:
            conditional_fields = json.loads(args.conditional_fields)
        except json.JSONDecodeError:
            logger.error("Invalid JSON for conditional_fields, using default")
    
    # Parse properties schema if provided
    properties_schema = DEFAULT_EVENT_PROPERTIES
    if args.properties:
        try:
            properties_schema = json.loads(args.properties)
        except json.JSONDecodeError:
            logger.error("Invalid JSON for properties, using default")
    
    generator = EventsGenerator(
        kafka_broker=args.kafka_broker,
        kafka_topic=args.kafka_topic,
        site=args.site,
        event_names=event_names,
        properties=properties_schema,
        total_messages=args.total_messages,
        target_true_count=args.target_true_count,
        conditional_fields=conditional_fields,
        num_threads=args.num_threads
    )
    generator.run()
