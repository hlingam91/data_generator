"""
Kafka Consumer for User Properties
Consumes messages from seg_poc_identity topic and extracts unique BSINs.
Writes all unique BSINs to a file in the data folder.

Sample message format:
{"insert": {"site_id": "boomtrain","bsin": "0002ac00-83ce-4071-bcd2-7a1089812b621","user_id": "6900077", ...}}
{"delete": {"site_id": "boomtrain","bsin": "0002ac00-83ce-4071-bcd2-7a1089812b621","user_id": "6900077", ...}}
"""

import json
import logging
import argparse
import os
from kafka import KafkaConsumer
import signal
import sys

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.config_loader import ConfigLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
max_records=1500000

class UserPropsConsumer:
    """Consumer to extract unique BSINs from user properties messages"""
    
    def __init__(self, bootstrap_servers="localhost:9092", topic="seg_poc_identity", group_id="user_props_consumer_group", output_file="data/bsins_extracted.txt"):
        """
        Initialize Kafka Consumer
        
        Args:
            bootstrap_servers: Kafka broker address
            topic: Topic name to consume from
            group_id: Consumer group ID
            output_file: Path to output file for BSINs
            max_records: Maximum number of records to process before stopping
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.output_file = output_file
        self.max_records = max_records
        self.pending_records = []  # Buffer for records to write
        self.total_messages = 0
        self.records_processed = 0
        self.insert_count = 0
        self.delete_count = 0
        self.running = True
        self.write_interval = 1000  # Write to file every N messages
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Clear/create output file at start
        self._initialize_output_file()
        
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=10000,  # Timeout after 10 seconds when no messages available
                value_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            logger.info(f"Kafka Consumer initialized for topic '{self.topic}' on {self.bootstrap_servers}")
            logger.info(f"Will stop after processing {self.max_records} records")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Consumer: {e}")
            raise
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def _initialize_output_file(self):
        """Initialize the output file directory"""
        try:
            # Ensure data directory exists
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            logger.info(f"Output file ready (append mode): {self.output_file}")
        except Exception as e:
            logger.error(f"Failed to initialize output file: {e}")
            raise
    
    def _process_message(self, message):
        """
        Process a single Kafka message and write to file
        
        Args:
            message: KafkaConsumer message object
        """
        try:
            value_str = message.value
            if not value_str:
                logger.warning(f"Message at offset {message.offset} has empty value, skipping")
                return
            
            try:
                # Handle multiple newline-separated JSON objects in a single message
                for msg in value_str.strip(" ").strip("\n").split("\n"):
                    if self.records_processed >= self.max_records:
                        self.running = False
                        return
                    
                    value_json = json.loads(msg)

                    # Check for insert or delete operation
                    if "insert" in value_json:
                        self.insert_count += 1
                        self.pending_records.append(msg)
                        self.records_processed += 1
                    elif "delete" in value_json:
                        self.delete_count += 1
                    
                    # Write to file and log progress every N records or if we hit the limit
                    if self.records_processed % self.write_interval == 0 or self.records_processed >= self.max_records:
                        self._flush_records_to_file()
                        logger.info(f"Processed {self.records_processed}/{self.max_records} records...")
                        
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON value at offset {message.offset}: {e}")
                return
                
            self.total_messages += 1
            
        except Exception as e:
            logger.error(f"Error processing message at offset {message.offset}: {e}")
            raise
    
    def consume(self):
        """Start consuming messages"""
        logger.info(f"Starting to consume messages from topic '{self.topic}'...")
        logger.info("Will stop automatically when no more messages are available (or press Ctrl+C)")
        
        messages_consumed = False
        try:
            while self.running:
                try:
                    for message in self.consumer:
                        if not self.running:
                            break
                        self._process_message(message)
                        messages_consumed = True
                except StopIteration:
                    # Consumer timeout reached - no more messages available
                    if messages_consumed:
                        logger.info("No more messages available in topic. Stopping consumer...")
                    else:
                        logger.info("No messages found in topic.")
                    break
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
        finally:
            self._close()
    
    def _close(self):
        """Close consumer, write remaining records to file, and print report"""
        logger.info("Closing Kafka Consumer...")
        if self.consumer:
            self.consumer.close()
        
        # Write any remaining records to file
        self._flush_records_to_file()
        
        # Print report
        self._print_report()
    
    def _flush_records_to_file(self):
        """Flush pending records to output file"""
        try:
            if not self.pending_records:
                return  # Nothing to write
            
            # Ensure data directory exists
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Append pending records to file
            with open(self.output_file, 'a') as f:
                for record in self.pending_records:
                    f.write(f"{record}\n")
            
            logger.debug(f"Flushed {len(self.pending_records)} records to {self.output_file}")
            
            # Clear the pending buffer
            self.pending_records.clear()
            
        except Exception as e:
            logger.error(f"Failed to flush records to file: {e}")
    
    def _print_report(self):
        """Print final report"""
        print("\n" + "="*70)
        print("USER PROPERTIES CONSUMER REPORT")
        print("="*70)
        print(f"Total Messages Processed: {self.total_messages}")
        print(f"Records Processed: {self.records_processed}")
        print(f"Insert Operations: {self.insert_count}")
        print(f"Delete Operations: {self.delete_count}")
        print(f"Output File: {self.output_file}")
        print("="*70 + "\n")


if __name__ == "__main__":
    # Load configuration
    ConfigLoader.load()
    default_brokers = ConfigLoader.get_kafka_brokers()

    parser = argparse.ArgumentParser(description='Consume user properties and write records to file')
    parser.add_argument('--bootstrap-servers', type=str, default=default_brokers, 
                        help=f'Kafka bootstrap servers (default: {default_brokers})')
    parser.add_argument('--topic', type=str, default='seg_poc_identity_narrow', 
                        help='Kafka topic to consume from (default: seg_poc_identity_narrow)')
    parser.add_argument('--group-id', type=str, default='user_props_consumer_group', 
                        help='Consumer group ID (default: user_props_consumer_group)')
    parser.add_argument('--output-file', type=str, default='data/users_extracted_new.txt',
                        help='Output file path for records (default: data/users_extracted.txt)')
    
    args = parser.parse_args()
    
    consumer = UserPropsConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        output_file=args.output_file,
    )
    
    consumer.consume()