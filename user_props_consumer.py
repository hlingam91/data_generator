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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
BOOTSTRAP_SERVERS = "b-1.preprod-msk-zme.95l1o5.c3.kafka.us-east-1.amazonaws.com:9092,b-2.preprod-msk-zme.95l1o5.c3.kafka.us-east-1.amazonaws.com:9092,b-3.preprod-msk-zme.95l1o5.c3.kafka.us-east-1.amazonaws.com:9092"

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
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.output_file = output_file
        self.written_bsins = set()  # Track BSINs already written to file
        self.pending_bsins = []  # Buffer for BSINs to write
        self.total_messages = 0
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
                consumer_timeout_ms=5000,  # Timeout after 5 seconds when no messages available
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
    
    def _initialize_output_file(self):
        """Initialize/clear the output file at the start"""
        try:
            # Ensure data directory exists
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Create empty file (clear if exists)
            with open(self.output_file, 'w') as f:
                pass
            
            logger.info(f"Initialized output file: {self.output_file}")
        except Exception as e:
            logger.error(f"Failed to initialize output file: {e}")
            raise
    
    def _process_message(self, message):
        """
        Process a single Kafka message and extract BSIN
        
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
                    value_json = json.loads(msg)
                    
                    # Check for insert or delete operation and extract BSIN
                    bsin = None
                    if "insert" in value_json:
                        bsin = value_json["insert"].get("bsin")
                        self.insert_count += 1
                    elif "delete" in value_json:
                        bsin = value_json["delete"].get("bsin")
                        self.delete_count += 1
                    else:
                        logger.warning(f"Message at offset {message.offset} has neither 'insert' nor 'delete' key")
                    
                    if bsin and bsin not in self.written_bsins:
                        self.pending_bsins.append(bsin)
                        self.written_bsins.add(bsin)
                        logger.debug(f"Extracted BSIN: {bsin}")
                    
                    self.total_messages += 1
                    
                    # Write to file and log progress every 1000 messages
                    if self.total_messages % self.write_interval == 0:
                        self._flush_bsins_to_file()
                        logger.info(f"Processed {self.total_messages} messages, found {len(self.written_bsins)} unique BSINs...")
                        
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON value at offset {message.offset}: {e}")
                return
                
        except Exception as e:
            logger.error(f"Error processing message at offset {message.offset}: {e}")
    
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
        """Close consumer, write remaining BSINs to file, and print report"""
        logger.info("Closing Kafka Consumer...")
        if self.consumer:
            self.consumer.close()
        
        # Write any remaining BSINs to file
        self._flush_bsins_to_file()
        
        # Print report
        self._print_report()
    
    def _flush_bsins_to_file(self):
        """Flush pending BSINs to output file"""
        try:
            if not self.pending_bsins:
                return  # Nothing to write
            
            # Ensure data directory exists
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Append pending BSINs to file
            with open(self.output_file, 'a') as f:
                for bsin in sorted(self.pending_bsins):
                    f.write(f"{bsin}\n")
            
            logger.debug(f"Flushed {len(self.pending_bsins)} BSINs to {self.output_file}")
            
            # Clear the pending buffer
            self.pending_bsins.clear()
            
        except Exception as e:
            logger.error(f"Failed to flush BSINs to file: {e}")
    
    def _print_report(self):
        """Print final report"""
        print("\n" + "="*70)
        print("USER PROPERTIES CONSUMER REPORT")
        print("="*70)
        print(f"Total Messages Processed: {self.total_messages}")
        print(f"Insert Operations: {self.insert_count}")
        print(f"Delete Operations: {self.delete_count}")
        print(f"Unique BSINs Written: {len(self.written_bsins)}")
        print(f"Output File: {self.output_file}")
        print("="*70 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Consume user properties and extract unique BSINs')
    parser.add_argument('--bootstrap-servers', type=str, default=BOOTSTRAP_SERVERS, 
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='seg_poc_identity', 
                        help='Kafka topic to consume from (default: seg_poc_identity)')
    parser.add_argument('--group-id', type=str, default='user_props_consumer_group', 
                        help='Consumer group ID (default: user_props_consumer_group)')
    parser.add_argument('--output-file', type=str, default='data/bsins_extracted.txt',
                        help='Output file path for BSINs (default: data/bsins_extracted.txt)')
    
    args = parser.parse_args()
    
    consumer = UserPropsConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        output_file=args.output_file
    )
    
    consumer.consume()