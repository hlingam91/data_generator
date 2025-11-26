from kafka import KafkaProducer as KP
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaProducer:
    """Class to handle producing messages to Kafka topic asynchronously"""
    
    def __init__(self, bootstrap_servers="localhost:9092"):
        """
        Initialize Kafka Producer
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
        """
        try:
            self.kafka_producer = KP(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode('utf-8'),
                acks=1,  # Wait for leader acknowledgment
                retries=10,  # Increased retries for reliability
                max_in_flight_requests_per_connection=5,  # Reduced to prevent overwhelming broker
                request_timeout_ms=120000,  # Reduced to 120 seconds - fail faster
                delivery_timeout_ms=130000,  # Reduced to 130 seconds total
                max_block_ms=180000,  # Reduced to 180 seconds - fail faster if buffer full
                api_version_auto_timeout_ms=10000,
                linger_ms=10,  # Reduced batching delay for faster sends
                batch_size=32768,  # 32KB batch size (smaller for faster sends)
                compression_type='lz4',  # Added compression to reduce network load
                buffer_memory=67108864  # 64MB buffer (reduced to apply backpressure sooner)
            )
            logger.info(f"Kafka Producer initialized with bootstrap_servers: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")
            raise

    def produce(self, topic, message):
        """
        Send message to Kafka topic asynchronously
        
        Args:
            topic: Kafka topic name
            message: Message to send (dict, str, or bytes)
        """
        try:
            future = self.kafka_producer.send(topic, message)
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
        except Exception as e:
            logger.error(f"Failed to send message to topic {topic}: {e}")
            raise

    def send(self, topic, message):
        """
        Send message to Kafka topic (alias for produce)
        
        Args:
            topic: Kafka topic name
            message: Message to send (dict, str, or bytes)
        """
        self.produce(topic, message)

    def flush(self):
        """Flush any pending messages"""
        if self.kafka_producer:
            self.kafka_producer.flush()
            logger.debug("Kafka Producer flushed")

    def close(self):
        """Close the Kafka Producer connection"""
        if self.kafka_producer:
            self.kafka_producer.close()
            logger.info("Kafka Producer closed")

    @staticmethod
    def _on_send_success(record_metadata):
        """Callback for successful message send"""
        logger.debug(f"Message sent successfully to {record_metadata.topic} "
                    f"partition {record_metadata.partition} "
                    f"at offset {record_metadata.offset}")

    @staticmethod
    def _on_send_error(exception):
        """Callback for failed message send"""
        logger.error(f"Failed to send message: {exception}")
