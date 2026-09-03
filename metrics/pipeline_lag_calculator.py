"""
Feldera Pipeline Lag Calculator

This script fetches Feldera pipeline metrics and calculates the lag 
between Kafka's latest offset and Feldera's consumed offset.

Features:
- Fetches pipeline stats from Feldera API
- Gets latest Kafka offsets for all topic partitions
- Calculates and displays lag metrics
- Supports both config file and command-line arguments
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import requests
from kafka import KafkaConsumer, TopicPartition

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config_loader import ConfigLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FelderaPipelineLagCalculator:
    """Calculate lag between Kafka and Feldera pipeline"""
    
    def __init__(
        self,
        kafka_brokers: str,
        feldera_api_url: str,
        pipeline_name: str,
        topics: List[str],
        api_key: Optional[str] = None
    ):
        """
        Initialize the lag calculator
        
        Args:
            kafka_brokers: Comma-separated Kafka broker addresses
            feldera_api_url: Base URL for Feldera API
            pipeline_name: Name of the Feldera pipeline
            topics: Deprecated; connector topics are discovered from Feldera
            api_key: Optional API key for Feldera authentication
        """
        self.kafka_brokers = kafka_brokers
        self.feldera_api_url = feldera_api_url.rstrip('/')
        self.pipeline_name = pipeline_name
        self.api_key = api_key
        
        logger.info(f"Initialized calculator for pipeline: {pipeline_name}")
        if topics:
            logger.info(
                "Configured topics are retained for CLI compatibility; "
                "pipeline connector topics will be discovered from Feldera"
            )

    def _headers(self) -> Dict[str, str]:
        """Build headers for Feldera API requests."""
        if self.api_key:
            return {'Authorization': f"Bearer {self.api_key}"}
        return {}

    def get_feldera_pipeline_definition(self) -> Dict:
        """Fetch the pipeline definition, including compiled input connectors."""
        logger.info(f"Fetching Feldera pipeline definition for: {self.pipeline_name}")
        url = f"{self.feldera_api_url}/v0/pipelines/{self.pipeline_name}"

        response = requests.get(url, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return response.json()

    def extract_kafka_connectors(self, pipeline: Dict) -> Dict[str, Dict]:
        """Return Kafka connector configuration keyed by Feldera endpoint name."""
        input_connectors = (
            pipeline.get('program_info', {}).get('input_connectors', {})
        )
        connectors = {}

        for endpoint_name, connector in input_connectors.items():
            transport = connector.get('transport', {})
            if transport.get('name') != 'kafka_input':
                continue

            config = transport.get('config', {})
            topic = config.get('topic')
            if not topic:
                logger.warning(
                    f"Skipping Kafka connector '{endpoint_name}' without a topic"
                )
                continue

            connectors[endpoint_name] = {
                'topic': topic,
                'brokers': config.get('bootstrap.servers') or self.kafka_brokers,
                'partitions': config.get('partitions'),
                'paused': connector.get('paused', False),
            }

        if not connectors:
            raise ValueError(
                f"No Kafka input connectors found in pipeline '{self.pipeline_name}'"
            )

        logger.info(
            f"Discovered {len(connectors)} Kafka input connectors from Feldera"
        )
        return connectors
    
    def get_kafka_latest_offsets(
        self, connectors: Dict[str, Dict]
    ) -> Dict[str, Dict[int, int]]:
        """
        Get latest offsets for every Kafka input connector.
        
        Returns:
            Dictionary mapping endpoint name -> {partition: offset}
        """
        logger.info("Fetching latest Kafka offsets...")
        offsets = {}
        
        for endpoint_name, connector in connectors.items():
            topic = connector['topic']
            consumer = None
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=connector['brokers'],
                    consumer_timeout_ms=5000
                )
                partitions = connector['partitions']
                if partitions is None:
                    partitions = consumer.partitions_for_topic(topic)

                if partitions is None:
                    logger.warning(f"Topic '{topic}' not found or has no partitions")
                    continue

                topic_partitions = [
                    TopicPartition(topic, partition) for partition in partitions
                ]
                end_offsets = consumer.end_offsets(topic_partitions)
                offsets[endpoint_name] = {
                    tp.partition: offset for tp, offset in end_offsets.items()
                }
            except Exception as e:
                logger.error(
                    f"Error fetching Kafka offsets for connector "
                    f"'{endpoint_name}' (topic '{topic}'): {e}"
                )
                raise
            finally:
                if consumer is not None:
                    consumer.close()

        logger.info(f"Successfully fetched offsets for {len(offsets)} connectors")
        return offsets
    
    def get_feldera_pipeline_stats(self) -> Dict:
        """
        Fetch pipeline statistics from Feldera API
        
        Returns:
            Dictionary containing pipeline stats
        """
        logger.info(f"Fetching Feldera pipeline stats for: {self.pipeline_name}")
        
        url = f"{self.feldera_api_url}/v0/pipelines/{self.pipeline_name}/stats"
         
        try:
            response = requests.get(url, headers=self._headers(), timeout=30)
            response.raise_for_status()
            
            stats = response.json()
            logger.info("Successfully fetched Feldera pipeline stats")
            return stats
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Feldera stats: {e}")
            raise
    
    def extract_feldera_offsets(
        self, stats: Dict, connectors: Dict[str, Dict]
    ) -> Dict[str, Dict[int, int]]:
        """
        Extract consumed offsets from Feldera stats
        
        Args:
            stats: Feldera pipeline stats dictionary
            
        Returns:
            Dictionary mapping endpoint name -> {partition: offset}
        """
        logger.info("Extracting Feldera consumed offsets from stats...")
        feldera_offsets = {endpoint_name: {} for endpoint_name in connectors}

        for input_stats in stats.get('inputs', []):
            endpoint_name = input_stats.get('endpoint_name')
            connector = connectors.get(endpoint_name)
            if connector is None:
                continue

            frontier = input_stats.get('completed_frontier')
            metadata = frontier.get('metadata', {}) if frontier else {}
            offset_ranges = metadata.get('offsets')
            if not offset_ranges:
                logger.warning(
                    f"Connector '{endpoint_name}' has no completed Kafka frontier"
                )
                continue

            configured_partitions = connector.get('partitions')
            if configured_partitions is None:
                partition_ids = list(range(len(offset_ranges)))
            else:
                partition_ids = configured_partitions
                if len(partition_ids) != len(offset_ranges):
                    logger.warning(
                        f"Connector '{endpoint_name}' reports {len(offset_ranges)} "
                        f"offsets for {len(partition_ids)} configured partitions"
                    )

            feldera_offsets[endpoint_name] = {
                partition: offset_range['end']
                for partition, offset_range in zip(partition_ids, offset_ranges)
            }

        logger.info(
            f"Extracted offsets for {len(feldera_offsets)} connectors from Feldera"
        )
        return feldera_offsets
    
    def calculate_lag(
        self,
        kafka_offsets: Dict[str, Dict[int, int]],
        feldera_offsets: Dict[str, Dict[int, int]]
    ) -> Dict[str, Dict[int, Optional[int]]]:
        """
        Calculate lag between Kafka and Feldera offsets
        
        Args:
            kafka_offsets: Kafka latest offsets by connector and partition
            feldera_offsets: Feldera consumed offsets by connector and partition
            
        Returns:
            Dictionary mapping endpoint name -> {partition: lag}; lag is None
            when Feldera has not reported a completed offset.
        """
        logger.info("Calculating lag...")
        lag = {}
        
        for endpoint_name, kafka_partitions in kafka_offsets.items():
            lag[endpoint_name] = {}
            feldera_partitions = feldera_offsets.get(endpoint_name, {})
            
            for partition, kafka_offset in kafka_partitions.items():
                feldera_offset = feldera_partitions.get(partition)
                partition_lag = (
                    kafka_offset - feldera_offset
                    if feldera_offset is not None
                    else None
                )
                lag[endpoint_name][partition] = partition_lag
                
                logger.debug(
                    f"Connector: {endpoint_name}, Partition: {partition}, "
                    f"Kafka: {kafka_offset}, Feldera: {feldera_offset}, Lag: {partition_lag}"
                )
        
        return lag
    
    def print_report(
        self,
        kafka_offsets: Dict[str, Dict[int, int]],
        feldera_offsets: Dict[str, Dict[int, int]],
        lag: Dict[str, Dict[int, Optional[int]]],
        pipeline_stats: Dict,
        connectors: Dict[str, Dict]
    ):
        """
        Print comprehensive lag report
        
        Args:
            kafka_offsets: Kafka latest offsets
            feldera_offsets: Feldera consumed offsets
            lag: Calculated lag
            pipeline_stats: Full pipeline stats from Feldera
        """
        print("\n" + "=" * 80)
        print(f"FELDERA PIPELINE LAG REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print(f"Pipeline: {self.pipeline_name}")
        print(f"Default Kafka Brokers: {self.kafka_brokers}")
        print(f"Feldera API: {self.feldera_api_url}")
        print("-" * 80)
        
        # Pipeline status
        if 'status' in pipeline_stats:
            print(f"Pipeline Status: {pipeline_stats['status']}")
        
        # Overall metrics
        total_lag = sum(
            partition_lag
            for partitions in lag.values()
            for partition_lag in partitions.values()
            if partition_lag is not None
        )
        missing_feldera_offsets = sum(
            partition_lag is None
            for partitions in lag.values()
            for partition_lag in partitions.values()
        )
        
        print(f"\nOVERALL METRICS:")
        print(f"  Total Lag (reported partitions): {total_lag:,}")
        print(f"  Partitions Without Feldera Offset: {missing_feldera_offsets:,}")
        
        # Per-topic breakdown
        print("\n" + "-" * 80)
        print("LAG BY TOPIC AND PARTITION:")
        print("-" * 80)
        
        for endpoint_name in sorted(kafka_offsets.keys()):
            kafka_partitions = kafka_offsets[endpoint_name]
            feldera_partitions = feldera_offsets.get(endpoint_name, {})
            lag_partitions = lag[endpoint_name]
            
            topic_total_lag = sum(
                value for value in lag_partitions.values() if value is not None
            )
            
            print(f"\nConnector: {endpoint_name}")
            print(f"  Topic: {connectors[endpoint_name]['topic']}")
            print(f"  Total Lag: {topic_total_lag:,}")
            print(f"  {'Partition':<12} {'Kafka Offset':<18} {'Feldera Offset':<18} {'Lag':<12}")
            print(f"  {'-'*12} {'-'*18} {'-'*18} {'-'*12}")
            
            for partition in sorted(kafka_partitions.keys()):
                kafka_off = kafka_partitions[partition]
                feldera_off = feldera_partitions.get(partition)
                partition_lag = lag_partitions[partition]
                
                # Color code lag (if high)
                lag_indicator = (
                    "⚠️ " if partition_lag is not None and partition_lag > 10000
                    else "  "
                )
                feldera_display = (
                    f"{feldera_off:,}" if feldera_off is not None else "N/A"
                )
                lag_display = (
                    f"{partition_lag:,}" if partition_lag is not None else "N/A"
                )
                
                print(
                    f"  {lag_indicator}{partition:<10} {kafka_off:<18,} "
                    f"{feldera_display:<18} {lag_display:<12}"
                )
        
        # Additional pipeline metrics
        if 'metrics' in pipeline_stats or 'global_metrics' in pipeline_stats:
            print("\n" + "-" * 80)
            print("ADDITIONAL PIPELINE METRICS:")
            print("-" * 80)
            
            metrics = pipeline_stats.get('metrics', pipeline_stats.get('global_metrics', {}))
            
            if 'total_input_records' in metrics:
                print(f"  Total Input Records: {metrics['total_input_records']:,}")
            if 'total_processed_records' in metrics:
                print(f"  Total Processed Records: {metrics['total_processed_records']:,}")
            if 'processing_rate' in metrics:
                print(f"  Processing Rate: {metrics['processing_rate']:,.2f} records/sec")
            if 'uptime_seconds' in metrics:
                uptime = metrics['uptime_seconds']
                hours = int(uptime // 3600)
                minutes = int((uptime % 3600) // 60)
                seconds = int(uptime % 60)
                print(f"  Uptime: {hours}h {minutes}m {seconds}s")
        
        print("\n" + "=" * 80 + "\n")
    
    def run(self):
        """Execute the lag calculation and print report"""
        try:
            pipeline_definition = self.get_feldera_pipeline_definition()
            connectors = self.extract_kafka_connectors(pipeline_definition)

            # Fetch Feldera pipeline stats
            pipeline_stats = self.get_feldera_pipeline_stats()
            
            # Extract Feldera offsets from stats
            feldera_offsets = self.extract_feldera_offsets(
                pipeline_stats, connectors
            )

            # Fetch Kafka offsets after the Feldera snapshot so that offsets
            # produced during collection cannot create a negative lag.
            kafka_offsets = self.get_kafka_latest_offsets(connectors)
            
            # Calculate lag
            lag = self.calculate_lag(kafka_offsets, feldera_offsets)
            
            # Print comprehensive report
            self.print_report(
                kafka_offsets, feldera_offsets, lag, pipeline_stats, connectors
            )
            
            return {
                'kafka_offsets': kafka_offsets,
                'feldera_offsets': feldera_offsets,
                'lag': lag,
                'pipeline_stats': pipeline_stats
            }
            
        except Exception as e:
            logger.error(f"Error during lag calculation: {e}")
            raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Calculate lag between Kafka and Feldera pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use config file (default)
  python pipeline_lag_calculator.py

  # Override with command-line arguments
  python pipeline_lag_calculator.py \\
    --kafka-brokers localhost:9092 \\
    --feldera-url https://feldera.example.com \\
    --pipeline my_pipeline \\
    --topics topic1,topic2  # Deprecated; topics are discovered from Feldera

  # With API key authentication
  python pipeline_lag_calculator.py --api-key YOUR_API_KEY
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    parser.add_argument(
        '--kafka-brokers',
        type=str,
        help='Kafka bootstrap servers (overrides config)'
    )
    parser.add_argument(
        '--feldera-url',
        type=str,
        help='Feldera API base URL (overrides config)'
    )
    parser.add_argument(
        '--pipeline',
        type=str,
        help='Feldera pipeline name (overrides config)'
    )
    parser.add_argument(
        '--topics',
        type=str,
        help='Deprecated; Kafka topics are discovered from pipeline connectors'
    )
    parser.add_argument(
        '--api-key',
        type=str,
        help='Feldera API key for authentication (overrides config)'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose debug logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load configuration
    try:
        ConfigLoader.load(args.config)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)
    
    # Get settings from config or command-line args
    kafka_brokers = args.kafka_brokers or ConfigLoader.get_kafka_brokers()
    feldera_url = args.feldera_url or ConfigLoader.get('feldera.api_url')
    pipeline_name = args.pipeline or ConfigLoader.get('feldera.pipeline_name')
    api_key = args.api_key or ConfigLoader.get('feldera.api_key')
    
    # Get topics
    if args.topics:
        topics = [t.strip() for t in args.topics.split(',')]
    else:
        # Get all topics from config
        topics_config = ConfigLoader.get('kafka.topics', {})
        if isinstance(topics_config, dict):
            topics = list(topics_config.values())
        else:
            topics = []
    
    # Validate required settings
    if not feldera_url:
        logger.error("Feldera API URL not provided. Set in config or use --feldera-url")
        sys.exit(1)
    
    if not pipeline_name:
        logger.error("Pipeline name not provided. Set in config or use --pipeline")
        sys.exit(1)
    
    # Create calculator and run
    calculator = FelderaPipelineLagCalculator(
        kafka_brokers=kafka_brokers,
        feldera_api_url=feldera_url,
        pipeline_name=pipeline_name,
        topics=topics,
        api_key=api_key
    )
    
    try:
        calculator.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to calculate lag: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
