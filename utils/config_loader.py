"""
Configuration loader utility to read settings from config file
"""
import json
import os
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Load and manage configuration from JSON file"""
    
    _config = None
    _config_file = "config.json"
    
    @classmethod
    def load(cls, config_file=None):
        """
        Load configuration from JSON file
        
        Args:
            config_file: Path to config file (optional, defaults to config.json in project root)
        
        Returns:
            dict: Configuration dictionary
        """
        if cls._config is not None:
            return cls._config
        
        if config_file:
            cls._config_file = config_file
        
        # Look for config file in project root
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, cls._config_file)
        
        try:
            with open(config_path, 'r') as f:
                cls._config = json.load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return cls._config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
    
    @classmethod
    def get(cls, key_path, default=None):
        """
        Get configuration value by key path
        
        Args:
            key_path: Dot-separated path to config value (e.g., "kafka.brokers")
            default: Default value if key not found
        
        Returns:
            Configuration value or default
        """
        if cls._config is None:
            cls.load()
        
        keys = key_path.split('.')
        value = cls._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    @classmethod
    def get_kafka_brokers(cls):
        """Get Kafka broker addresses"""
        return cls.get("kafka.brokers", "localhost:9092")
    
    @classmethod
    def get_kafka_topic(cls, topic_name):
        """
        Get Kafka topic by name
        
        Args:
            topic_name: Topic key name (e.g., "events", "identity")
        
        Returns:
            Topic name string
        """
        return cls.get(f"kafka.topics.{topic_name}", topic_name)
    
    @classmethod
    def get_bsin_file(cls):
        """Get BSIN file path"""
        return cls.get("data.bsin_file", "data/bsins_extracted.txt")
    
    @classmethod
    def get_default(cls, key, fallback=None):
        """
        Get default configuration value
        
        Args:
            key: Key name in defaults section
            fallback: Fallback value if not found
        
        Returns:
            Default value or fallback
        """
        return cls.get(f"defaults.{key}", fallback)

