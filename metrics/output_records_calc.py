"""
Feldera Pipeline Metrics Calculator

This script fetches Feldera pipeline metrics to:
1. Calculate total transmitted records across all outputs
2. Calculate lag between Kafka's latest offset and Feldera's consumed offset

Output format parsing focuses on:
 "outputs": [
        {
            "endpoint_name": "...",
            "metrics": {
                "transmitted_records": 21978800,
                ...
            }
        }
    ]

Features:
- parses pipeline stats from Feldera API
- Counts total transmitted records from all output endpoints
- Gets latest Kafka offsets and calculates lag
- Supports both config file and command-line arguments
"""

import sys
import os
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import requests
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config_loader import ConfigLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FelderaPipelineMetricsCalculator:
    """Calculate metrics including output records and lag for Feldera pipeline"""
    
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
            topics: List of Kafka topics to monitor
            api_key: Optional API key for Feldera authentication
        """
        self.kafka_brokers = kafka_brokers
        self.feldera_api_url = feldera_api_url.rstrip('/')
        self.pipeline_name = pipeline_name
        self.topics = topics
        self.api_key = api_key
        
        logger.info(f"Initialized calculator for pipeline: {pipeline_name}")
        logger.info(f"Monitoring topics: {', '.join(topics)}")
    
    def get_kafka_latest_offsets(self) -> Dict[str, Dict[int, int]]:
        """
        Get latest offsets for all partitions of monitored topics
        
        Returns:
            Dictionary mapping topic -> {partition: offset}
        """
        logger.info("Fetching latest Kafka offsets...")
        offsets = {}
        
        try:
            # Create consumer to fetch offsets
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_brokers,
                consumer_timeout_ms=5000
            )
            
            # Collect all TopicPartitions to fetch in one call
            topic_partitions = []
            topic_partition_map = {}
            
            for topic in self.topics:
                # Get partitions for this topic
                partitions = consumer.partitions_for_topic(topic)
                
                if partitions is None:
                    logger.warning(f"Topic '{topic}' not found or has no partitions")
                    continue
                
                offsets[topic] = {}
                for partition in partitions:
                    tp = TopicPartition(topic, partition)
                    topic_partitions.append(tp)
                    topic_partition_map[tp] = (topic, partition)
            
            # Fetch all end offsets in a single efficient API call
            end_offsets = consumer.end_offsets(topic_partitions)
            
            # Map the results back to our structure
            for tp, offset in end_offsets.items():
                topic, partition = topic_partition_map[tp]
                offsets[topic][partition] = offset
                logger.debug(f"Topic: {topic}, Partition: {partition}, Latest Offset: {offset}")
            
            consumer.close()
            logger.info(f"Successfully fetched offsets for {len(offsets)} topics")
            return offsets
            
        except Exception as e:
            logger.error(f"Error fetching Kafka offsets: {e}")
            raise
    
    def get_feldera_pipeline_stats(self) -> Dict:
        """
        Fetch pipeline statistics from Feldera API
        
        Returns:
            Dictionary containing pipeline stats
        """
        logger.info(f"Fetching Feldera pipeline stats for: {self.pipeline_name}")
        
        url = f"{self.feldera_api_url}/v0/pipelines/{self.pipeline_name}/stats"
       
        headers = {}
        if self.api_key:
            headers['Authorization'] = f"Bearer {self.api_key}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            stats = response.json()
            logger.info("Successfully fetched Feldera pipeline stats")
            return stats
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Feldera stats: {e}")
            raise
    
    def extract_feldera_offsets(self, stats: Dict) -> Dict[str, Dict[int, int]]:
        """
        Extract consumed offsets from Feldera stats
        
        Args:
            stats: Feldera pipeline stats dictionary
            
        Returns:
            Dictionary mapping topic -> {partition: offset}
        """
        logger.info("Extracting Feldera consumed offsets from stats...")
        feldera_offsets = {"seg_poc_identity":{},
                           "seg_poc_events": {}
                           }

        try:
            # feldera offsets are in the "inputs": ["endpoint_name": "user_props_input.user_props",] -- "completed_frontier": {                 "metadata": {                     "offsets": [
                        # {
                        #     "end": 1410926,
                        #     "start": 1410926
                        # },]
            # Navigate through stats structure to find input connector metrics
            # Structure may vary - adjust based on actual API response
            if 'inputs' in stats:   
                for  idx, input_stats in enumerate(stats['inputs']):
                    if input_stats['endpoint_name'].startswith("user_props") or input_stats['endpoint_name'].startswith("events"):
                        topic_name = "seg_poc_identity" if input_stats['endpoint_name'].startswith("user_props") else "seg_poc_events"
                        kafka_offsets = input_stats['completed_frontier']['metadata']["offsets"]
                        
                        # # Extract topic and partition info
                        # if 'completed_frontier' in kafka_metrics:
                        for idx, item in enumerate(kafka_offsets):
                            item_end = item['end']
                            item_start = item['start']
                            feldera_offsets[topic_name][idx] = item_end
            
            logger.info(f"Extracted offsets for {len(feldera_offsets)} topics from Feldera")
            return feldera_offsets    
        except Exception as e:
            print(f"Error extracting Feldera offsets: {e}")
            logger.error(f"Error extracting Feldera offsets: {e}")
            logger.debug(f"Stats structure: {json.dumps(stats, indent=2)}")
            return {}
    
    def calculate_total_transmitted_records(self, stats: Dict) -> int:
        """
        Calculate total transmitted records from all output endpoints
        
        Args:
            stats: Feldera pipeline stats dictionary
            
        Returns:
            Total transmitted records count
        """
        logger.info("Calculating total transmitted records...")
        total_records = 0
        
        if 'outputs' in stats:
            for output in stats['outputs']:
                metrics = output.get('metrics', {})
                records = metrics.get('transmitted_records', 0)
                total_records += records
                
        logger.info(f"Total transmitted records: {total_records}")
        return total_records

    def calculate_lag(
        self,
        kafka_offsets: Dict[str, Dict[int, int]],
        feldera_offsets: Dict[str, Dict[int, int]]
    ) -> Dict[str, Dict[int, int]]:
        """
        Calculate lag between Kafka and Feldera offsets
        
        Args:
            kafka_offsets: Kafka latest offsets by topic and partition
            feldera_offsets: Feldera consumed offsets by topic and partition
            
        Returns:
            Dictionary mapping topic -> {partition: lag}
        """
        logger.info("Calculating lag...")
        lag = {}
        
        for topic, kafka_partitions in kafka_offsets.items():
            lag[topic] = {}
            feldera_partitions = feldera_offsets.get(topic, {})
            
            for partition, kafka_offset in kafka_partitions.items():
                feldera_offset = feldera_partitions.get(partition, 0)
                partition_lag = kafka_offset - feldera_offset
                lag[topic][partition] = partition_lag
                
                logger.debug(
                    f"Topic: {topic}, Partition: {partition}, "
                    f"Kafka: {kafka_offset}, Feldera: {feldera_offset}, Lag: {partition_lag}"
                )
        
        return lag
    
    def print_report(
        self,
        kafka_offsets: Dict[str, Dict[int, int]],
        feldera_offsets: Dict[str, Dict[int, int]],
        lag: Dict[str, Dict[int, int]],
        pipeline_stats: Dict,
        total_transmitted_records: int = 0
    ):
        """
        Print comprehensive lag report
        
        Args:
            kafka_offsets: Kafka latest offsets
            feldera_offsets: Feldera consumed offsets
            lag: Calculated lag
            pipeline_stats: Full pipeline stats from Feldera
            total_transmitted_records: Total records transmitted by pipeline outputs
        """
        print("\n" + "=" * 80)
        print(f"FELDERA PIPELINE METRICS REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print(f"Pipeline: {self.pipeline_name}")
        print(f"Kafka Brokers: {self.kafka_brokers}")
        print(f"Feldera API: {self.feldera_api_url}")
        print("-" * 80)
        
        # Output Metrics
        print(f"\nOUTPUT METRICS:")
        print(f"  Total Transmitted Records: {total_transmitted_records:,}")
        
        # Pipeline status
        if 'status' in pipeline_stats:
            print(f"Pipeline Status: {pipeline_stats['status']}")
        
        # Overall metrics
        total_lag = sum(sum(partitions.values()) for partitions in lag.values())
        total_kafka_messages = sum(
            sum(partitions.values()) for partitions in kafka_offsets.values()
        )
        total_feldera_consumed = sum(
            sum(partitions.values()) for partitions in feldera_offsets.values()
        )
        
        print(f"\nOVERALL METRICS:")
        print(f"  Total Kafka Messages: {total_kafka_messages:,}")
        print(f"  Total Feldera Consumed: {total_feldera_consumed:,}")
        print(f"  Total Lag: {total_lag:,}")
        
        if total_kafka_messages > 0:
            consumption_rate = (total_feldera_consumed / total_kafka_messages) * 100
            print(f"  Consumption Rate: {consumption_rate:.2f}%")
        
        # Per-topic breakdown
        print("\n" + "-" * 80)
        print("LAG BY TOPIC AND PARTITION:")
        print("-" * 80)
        
        for topic in sorted(kafka_offsets.keys()):
            kafka_partitions = kafka_offsets[topic]
            feldera_partitions = feldera_offsets.get(topic, {})
            lag_partitions = lag[topic]
            
            topic_total_lag = sum(lag_partitions.values())
            
            print(f"\nTopic: {topic}")
            print(f"  Total Lag: {topic_total_lag:,}")
            print(f"  {'Partition':<12} {'Kafka Offset':<18} {'Feldera Offset':<18} {'Lag':<12}")
            print(f"  {'-'*12} {'-'*18} {'-'*18} {'-'*12}")
            
            for partition in sorted(kafka_partitions.keys()):
                kafka_off = kafka_partitions[partition]
                feldera_off = feldera_partitions.get(partition, 0)
                partition_lag = lag_partitions[partition]
                
                # Color code lag (if high)
                lag_indicator = "⚠️ " if partition_lag > 10000 else "  "
                
                print(
                    f"  {lag_indicator}{partition:<10} {kafka_off:<18,} "
                    f"{feldera_off:<18,} {partition_lag:<12,}"
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
            # # Fetch Kafka offsets
            # kafka_offsets = self.get_kafka_latest_offsets()
            
            # Fetch Feldera pipeline stats
            pipeline_stats = self.get_feldera_pipeline_stats()
            
            # # Extract Feldera offsets from stats
            # feldera_offsets = self.extract_feldera_offsets(pipeline_stats)
            
            # # Calculate lag
            # lag = self.calculate_lag(kafka_offsets, feldera_offsets)
            
            # Calculate total transmitted records
            total_transmitted = self.calculate_total_transmitted_records(pipeline_stats)
            
            # Print comprehensive report
            # self.print_report(kafka_offsets, feldera_offsets, lag, pipeline_stats, total_transmitted)
            
            return {
                # 'kafka_offsets': kafka_offsets,
                # 'feldera_offsets': feldera_offsets,
                # 'lag': lag,
                # 'pipeline_stats': pipeline_stats,
                'total_transmitted_records': total_transmitted
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
    --topics topic1,topic2

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
        help='Comma-separated list of Kafka topics to monitor (overrides config)'
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
    
    if not topics:
        logger.error("No topics specified. Set in config or use --topics")
        sys.exit(1)
    
    # Create calculator and run
    calculator = FelderaPipelineMetricsCalculator(
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
