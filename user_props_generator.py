# This script generates synthetic data for user properties and pushes to kafka topic.
	# Inputs: Custom Fields with datatype n range selection for fields like dates, numbers, strings, booleans, etc. -- pushes to kafka messages in range/out of range
	# Output: how many messages in range/out of range were pushed to kafka
# Sample messages format:
# {"insert": {"site_id": "boomtrain","bsin": "0002ac00-83ce-4071-bcd2-7a1089812b621","user_id": "6900077","email": "","contacts": {},"properties": {  "has_active_email": "true","all_emails": "true", "double_optin": "true", "shopping_cart":{"total": 5000},"signed_up_at": "2025-10-15 23:05:00", "appointment_time": "2025-10-15 05:31:00", "last_purchase": {"datafields": {"total":200, "items": [{"item_id": 1, "item_name": "brush"}]}} },"last_updated": "2025-04-25 0:00:00","replaced_by": "","email_md5": "","sub_site_ids": [],"scoped_properties": {},"scoped_contacts": {},"imported_from": {},"consent": {},"external_ids": {},"unique_client_ids": [],"merged_bsins": []   }}
# {"insert": {"site_id": "boomtrain","bsin": "6test2fecc6aa9-81cc-4370-aba9-a35d95ae4969","user_id": "6900077","email": "","contacts": {"email::hlingam@zetaglobal.com":  { "type": "email","value": "hlingam@zetaglobal.com","status": "inactive","preferences": ["standard"],"inactivity_reason": "unsubscribed","created_at": 1536238279,"updated_at": 1701117523,"last_inactivity_updated_at": 1605806750,"last_clicked": null,"last_opened": null,"last_sent": 1701117491,"last_purchased": null,"domain": "zetaglobal.com","signed_up_at": 1536238279,"double_opt_in_status": null,"country_code": null,"area_code": null,"timezone": null,"geolocation": null,"phone_type": null,"contact_properties": null}}, "properties": { "has_active_email": "true", "shopping_cart":{"total": 10000},"signed_up_at": "2025-10-21 21:00:00","signed_up": "2025-10-21 21:00:00", "appointment_time": "2025-10-15 05:31:00"  },"last_updated": "2025-04-25 0:00:00","replaced_by": "","email_md5": "","sub_site_ids": [],"scoped_properties": {},"scoped_contacts":{},"imported_from": {},"consent": {},"external_ids": {},"unique_client_ids": [],"merged_bsins": []   }}

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


# Custom struct generators registry
# Add more custom structures here as needed
CUSTOM_STRUCT_GENERATORS = {
    "purchase": lambda faker: {
        "datafields": {
            "total": random.randint(100, 5000),
            "items": [
                {
                    "item_id": random.randint(1, 1000),
                    "item_name": faker.word()
                }
                for _ in range(random.randint(1, 5))  # Generate 1-5 items
            ]
        }
    },
    # Example of another custom structure for future use
    # "order": lambda faker: {
    #     "datafields": {
    #         "order_id": faker.uuid4(),
    #         "status": random.choice(["pending", "completed", "cancelled"]),
    #         "metadata": {"source": "web", "device": "mobile"}
    #     }
    # }
}

MESSAGE_COUNT = 1
CONDITIONAL_FIELDS = []
# CONDITIONAL_FIELDS = ["and", ["eq", "user.double_optin", "true"],["eq", "user.has_active_email", "true"],["eq", "user.all_emails", "true"], ["withinlast", "user.signed_up_at", "30", "days"]]

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

properties = {"has_active_email": "bool_str",
              "all_emails": "bool_str",
              "double_optin": "bool_str",
              "signed_up_at": "datetime",
              "appointment_time": "datetime",
              "last_purchase": "custom_struct:purchase"}

class UserPropsGenerator:
    def __init__(self, kafka_broker: str, kafka_topic: str, *args, **kwargs):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.args = args
        self.kwargs = kwargs
        self.site = kwargs.get("site", "boomtrain")
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        self.faker = Faker()
        self.properties_schema = kwargs.get("properties", properties)
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
                elif operator in ["eq","neq"]:
                    # Extract field path and expected value
                    field_path = cond[1].replace("user.", "properties.")
                    expected_value = cond[2]
                    return {"type": operator, "field": field_path, "value": expected_value}
                elif operator in ["withinlast","withinnext", "withinlastweek", "withinlastmonth", "withinlastyear"]:
                    # Extract field path and time range
                    field_path = cond[1].replace("user.", "properties.")
                    days = int(cond[2])
                    return {"type": operator, "field": field_path, "days": days}
            return None
        
        return parse_condition(self.conditional_fields)

    def _generate_value(self, field_name: str, data_type: str, condition_value=None):
        """Generate mock value based on data type"""
        # If a specific condition value is provided, use it
        if condition_value is not None:
            return condition_value
        
        # Handle custom struct types
        if data_type.startswith("custom_struct:"):
            struct_type = data_type.split(":", 1)[1]
            generator_func = CUSTOM_STRUCT_GENERATORS.get(struct_type)
            if generator_func:
                return generator_func(self.faker)
            else:
                logger.warning(f"Unknown custom_struct type: {struct_type}")
                return {}
        
        if data_type == "string":
            if field_name.lower().endswith("email"):
                return self.faker.email()
            elif field_name.lower().endswith("company"):
                return self.faker.company()
            elif field_name.lower().endswith("country"):
                return self.faker.country()
            elif field_name.lower().endswith("city"):
                return self.faker.city()
            elif field_name.lower().endswith("state"):
                return self.faker.state()
            elif field_name.lower() == "zip":
                return self.faker.zipcode()
            return self.faker.word()
        elif data_type == "uuid":
            return self.faker.uuid4()
        elif data_type == "email":
            return self.faker.email()
        elif data_type == "datetime":
            return self.faker.date_time_between(start_date='-30d', end_date='-30d').strftime('%Y-%m-%d %H:%M:%S')
        elif data_type == "bool_str":
            return str(random.choice([True, False])).lower()
        elif data_type.startswith("map<int>"):
            return {"total": random.randint(100, 10000)}
        elif data_type.startswith("map<Contact>"):
            # Generate a contact map with email as key
            email = self.faker.email()
            time = self.faker.date_time_between(start_date='-30d', end_date='-30d').strftime('%Y-%m-%d %H:%M:%S')
            return {
                
                f"email::{email}": {
                    "type": "email",
                    "value": email,
                    "status": random.choice(["active", "inactive"]),
                    "preferences": ["standard"],
                    "inactivity_reason": random.choice(["unsubscribed", None]),
                    "created_at": time,
                    "updated_at": time,
                    # "last_inactivity_updated_at": self.faker.unix_time(),
                    # "last_clicked": None,
                    # "last_opened": None,
                    # "last_sent": self.faker.unix_time(),
                    # "last_purchased": None,
                    # "domain": email.split('@')[1],
                    # "signed_up_at": self.faker.unix_time(),
                    # "double_opt_in_status": None,
                    # "country_code": None,
                    # "area_code": None,
                    # "timezone": None,
                    # "geolocation": None,
                    # "phone_type": None,
                    # "contact_properties": None
                }
            }
        else:
            return ""

    def _evaluate_condition(self, user_props, condition):
        """Evaluate if a condition is true for the given user properties"""
        if isinstance(condition, list):
            operator = condition[0] if condition else None
            if operator == "and":
                return all(self._evaluate_condition(user_props, c) for c in condition[1:])
            elif operator == "or":
                return any(self._evaluate_condition(user_props, c) for c in condition[1:])
        elif isinstance(condition, dict):
            cond_type = condition.get("type")
            field_path = condition.get("field")
            
            # Extract value from nested path
            value = user_props
            for key in field_path.split("."):
                value = value.get(key) if isinstance(value, dict) else None
                if value is None:
                    return False
            
            if cond_type == "eq":
                return str(value) == str(condition.get("value"))
            elif cond_type == "withinlast":
                try:
                    dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    days_ago = datetime.now() - timedelta(days=condition.get("days"))
                    return dt >= days_ago
                except:
                    return False
        return False

    def generate_user_props(self, should_match_condition=False):
        """Generate user properties record based on the fields and datatypes"""
        # Parse conditions
        parsed_conditions = self._parse_conditions()
        
        # Generate base fields from DEFAULT_MESSAGE_FIELDS
        user_props = {}
        for field, field_type in DEFAULT_MESSAGE_FIELDS.items():
            if field == "properties":
                # Generate properties hash based on properties_schema
                props = {}
                for prop_name, prop_type in self.properties_schema.items():
                    condition_value = None
                    
                    # Check if this field is part of conditions
                    if should_match_condition and parsed_conditions:
                        condition_value = self._get_condition_value(prop_name, parsed_conditions, True)
                    elif not should_match_condition and parsed_conditions:
                        condition_value = self._get_condition_value(prop_name, parsed_conditions, False)
                    
                    props[prop_name] = self._generate_value(prop_name, prop_type, condition_value)
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
                if field_path.endswith(field_name):
                    if cond["type"] == "eq":
                        return cond["value"] if should_match else ("false" if cond["value"] == "true" else "true")
                    elif cond["type"] == "withinlast":
                        if should_match:
                            # Generate date within the range
                            days = cond["days"]
                            dt = self.faker.date_time_between(start_date=f'-{days}d', end_date='now')
                            return dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            # Generate date outside the range
                            days = cond["days"]
                            dt = self.faker.date_time_between(start_date='-2y', end_date=f'-{days+1}d')
                            return dt.strftime('%Y-%m-%d %H:%M:%S')
            return None
        
        return extract_value(conditions)

    def push_to_kafka(self, user_props: dict):
        try:
            # print(user_props)
            self.kafka_producer.send(self.kafka_topic, json.dumps(user_props).encode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise

    def _worker_thread(self, message_queue: Queue):
        """Worker thread to generate and push messages"""
        parsed_conditions = self._parse_conditions()
        local_count = 0
        
        while True:
            task = message_queue.get()
            if task is None:  # Poison pill to stop the thread
                message_queue.task_done()
                break
            
            should_match = task
            
            # Generate user properties
            user_props = self.generate_user_props(should_match_condition=should_match)
            
            # Verify the condition evaluation (for tracking)
            condition_met = self._evaluate_condition(user_props["insert"], parsed_conditions) if parsed_conditions else False
            
            with self.count_lock:
                if condition_met:
                    self.true_count += 1
                else:
                    self.false_count += 1
            
            # Push to Kafka
            self.push_to_kafka(user_props)
            
            local_count += 1
            # Periodic flush to avoid buffering
            if local_count % self.flush_interval == 0:
                self.kafka_producer.flush()
            
            message_queue.task_done()

    def run(self):
        self.start_time = time.time()
        logger.info(f"Starting message generation: Total={self.total_messages}, Target True={self.target_true_count}, Threads={self.num_threads}")
        
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
                # Determine if this message should match the condition
                should_match = self.true_count < self.target_true_count
                
                # Generate user properties
                user_props = self.generate_user_props(should_match_condition=should_match)
                
                # Verify the condition evaluation (for tracking)
                condition_met = self._evaluate_condition(user_props["insert"], parsed_conditions) if parsed_conditions else False
                
                if condition_met:
                    self.true_count += 1
                else:
                    self.false_count += 1
                
                # Push to Kafka
                self.push_to_kafka(user_props)
                count += 1
                
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
        print(f"Messages with Condition TRUE: {self.true_count}")
        print(f"Messages with Condition FALSE: {self.false_count}")
        print(f"Target True Count: {self.target_true_count}")
        print(f"Actual Match Rate: {(self.true_count/self.total_messages)*100:.2f}%")
        print(f"Time Taken: {elapsed_time:.2f} seconds")
        print(f"Throughput: {messages_per_sec:.2f} messages/sec")
        print(f"Threads Used: {self.num_threads}")
        print("="*50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate user properties with conditional fields')
    parser.add_argument('--total-messages', type=int, default=100000000, help='Total number of messages to generate')
    parser.add_argument('--target-true-count', type=int, default=100000000, help='Target number of messages where condition is true')
    parser.add_argument('--kafka-broker', type=str, default="b-1.preprod-msk-zme.95l1o5.c3.kafka.us-east-1.amazonaws.com:9092,b-2.preprod-msk-zme.95l1o5.c3.kafka.us-east-1.amazonaws.com:9092,b-3.preprod-msk-zme.95l1o5.c3.kafka.us-east-1.amazonaws.com:9092", help='Kafka broker address')
    parser.add_argument('--kafka-topic', type=str, default='seg_poc_identity', help='Kafka topic name')
    parser.add_argument('--conditional-fields', type=str, default=None, help='JSON string of conditional fields')
    parser.add_argument('--num-threads', type=int, default=15, help='Number of threads for parallel generation')
    
    args = parser.parse_args()
    
    # Parse conditional fields if provided
    conditional_fields = CONDITIONAL_FIELDS
    if args.conditional_fields:
        try:
            conditional_fields = json.loads(args.conditional_fields)
        except json.JSONDecodeError:
            logger.error("Invalid JSON for conditional_fields, using default")
    
    generator = UserPropsGenerator(
        kafka_broker=args.kafka_broker,
        kafka_topic=args.kafka_topic,
        total_messages=args.total_messages,
        target_true_count=args.target_true_count,
        conditional_fields=conditional_fields,
        num_threads=args.num_threads
    )
    generator.run()
