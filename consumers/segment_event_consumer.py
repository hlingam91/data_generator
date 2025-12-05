"""
Kafka Consumer for Segment Events
Consumes messages from a Kafka topic and maintains running counts per view_name.
- "insert" in message value increments the count
- "delete" in message value decrements the count
- view_name is extracted from message headers
"""

import json
import logging
import argparse
from collections import defaultdict
from kafka import KafkaConsumer
import signal
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.config_loader import ConfigLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SegmentEventConsumer:
    """Consumer to track insert/delete operations per view_name"""
    
    def __init__(self, bootstrap_servers="localhost:9092", topic="seg_poc_segment_events", group_id="segment_consumer_group"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.view_counts = defaultdict(int)
        self.total_inserts = 0
        self.total_deletes = 0
        self.total_messages = 0
        self.running = True
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000,  # Timeout after 1 second to check for shutdown
                value_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            logger.info(f"Kafka Consumer initialized for topic '{self.topic}' on {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Consumer: {e}")
            raise
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def _extract_view_name(self, headers):
        """
        Extract view_name from message headers
        
        Args:
            headers: List of tuples (key, value) from Kafka message
            
        Returns:
            view_name string or None
        """
        if not headers:
            return None
        
        for key, value in headers:
            if key == "view_name":
                return value.decode('utf-8') if isinstance(value, bytes) else value
        return None
    
    def _process_message(self, message):
        """
        Process a single Kafka message
        
        Args:
            message: KafkaConsumer message object
        """
        try:
            # Extract view_name from headers
            view_name = self._extract_view_name(message.headers)
            if not view_name:
                logger.warning(f"Message at offset {message.offset} has no view_name header, skipping")
                return
            
            # Parse the value JSON
            value_str = message.value
            if not value_str:
                logger.warning(f"Message at offset {message.offset} has empty value, skipping")
                return
            
            try:
                for msg in value_str.strip(" " ).strip("\n").split("\n"):
                    value_json = json.loads(msg)
                    # Check for insert or delete operation
                    if "insert" in value_json:
                        self.view_counts[view_name] += 1
                        self.total_inserts += 1
                        logger.debug(f"INSERT for view_name='{view_name}', count now: {self.view_counts[view_name]}")
                    elif "delete" in value_json:
                        self.view_counts[view_name] -= 1
                        self.total_deletes += 1
                        logger.debug(f"DELETE for view_name='{view_name}', count now: {self.view_counts[view_name]}")
                    else:
                        logger.warning(f"Message at offset {message.offset} has neither 'insert' nor 'delete' key")

                    self.total_messages += 1
                    # Log progress every 100 messages
                    if self.total_messages % 1000 == 0:
                        logger.info(f"Processed {self.total_messages} messages...")
                        self._print_report()
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON value at offset {message.offset}: {e}")
                return

                
        except Exception as e:
            logger.error(f"Error processing message at offset {message.offset}: {e}")
    
    def consume(self):
        """Start consuming messages"""
        logger.info(f"Starting to consume messages from topic '{self.topic}'...")
        logger.info("Press Ctrl+C to stop and see the report")
        
        try:
            while self.running:
                try:
                    for message in self.consumer:
                        if not self.running:
                            break
                        self._process_message(message)
                except StopIteration:
                    # Consumer timeout reached, check running flag and continue
                    self._print_report()
                    continue
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
        finally:
            self._close()
    
    def _close(self):
        """Close consumer and print report"""
        logger.info("Closing Kafka Consumer...")
        if self.consumer:
            self.consumer.close()
        self._print_report()
    
    def _print_report(self):
        """Print final count report"""
        print("\n" + "="*70)
        print("SEGMENT EVENT CONSUMER REPORT")
        print("="*70)
        print(f"Total Messages Processed: {self.total_messages}")
        print(f"Total Inserts: {self.total_inserts}")
        print(f"Total Deletes: {self.total_deletes}")
        print(f"Net Change: {self.total_inserts - self.total_deletes}")
        print("\n" + "-"*70)
        print("COUNTS PER VIEW_NAME:")
        print("-"*70)
        
        if self.view_counts:
            # Sort by view_name for consistent output
            sorted_views = sorted(self.view_counts.items())
            for view_name, count in sorted_views:
                print(f"{view_name:40} : {count:10,}")
        else:
            print("No view_name counts recorded")
        
        print("="*70 + "\n")


if __name__ == "__main__":
    # Load configuration
    ConfigLoader.load()
    default_brokers = ConfigLoader.get_kafka_brokers()

    parser = argparse.ArgumentParser(description='Consume segment events and track counts per view_name')
    parser.add_argument('--bootstrap-servers', type=str, default=default_brokers, 
                        help=f'Kafka bootstrap servers (default: {default_brokers})')
    parser.add_argument('--topic', type=str, default='seg_poc_segment_events', 
                        help='Kafka topic to consume from (default: segment)')
    parser.add_argument('--group-id', type=str, default='segment_consumer_group1',
                        help='Consumer group ID (default: segment_consumer_group)')
    
    args = parser.parse_args()
    
    consumer = SegmentEventConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id
    )
    
    consumer.consume()