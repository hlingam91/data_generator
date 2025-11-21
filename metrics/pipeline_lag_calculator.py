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
        # return json.loads('''{
        #     "global_metrics": {
        #         "state": "Running",
        #         "bootstrap_in_progress": false,
        #         "transaction_status": "NoTransaction",
        #         "transaction_id": 0,
        #         "transaction_initiators": {
        #             "transaction_id": null,
        #             "initiated_by_api": null,
        #             "initiated_by_connectors": {}
        #         },
        #         "rss_bytes": 14300135424,
        #         "cpu_msecs": 26443546,
        #         "uptime_msecs": 3088572,
        #         "start_time": 1763491567,
        #         "incarnation_uuid": "019a9849-d72b-7983-baa6-c353769d27fa",
        #         "initial_start_time": 1763424403,
        #         "storage_bytes": 182861313166,
        #         "storage_mb_secs": 503962023,
        #         "runtime_elapsed_msecs": 46033544,
        #         "buffered_input_records": 1004635,
        #         "buffered_input_bytes": 1107579082,
        #         "total_input_records": 71638390,
        #         "total_input_bytes": 15280976944,
        #         "total_processed_records": 70613748,
        #         "total_processed_bytes": 14158429694,
        #         "total_completed_records": 70613748,
        #         "pipeline_complete": false
        #     },
        #     "suspend_error": null,
        #     "inputs": [
        #         {
        #             "endpoint_name": "events.events",
        #             "config": {
        #                 "stream": "events"
        #             },
        #             "metrics": {
        #                 "total_bytes": 631581367,
        #                 "total_records": 1615724,
        #                 "buffered_records": 4593,
        #                 "buffered_bytes": 1795991,
        #                 "num_transport_errors": 0,
        #                 "num_parse_errors": 0,
        #                 "end_of_input": false
        #             },
        #             "fatal_error": null,
        #             "paused": false,
        #             "barrier": false,
        #             "completed_frontier": {
        #                 "metadata": {
        #                     "offsets": [
        #                         {
        #                             "end": 66303,
        #                             "start": 65880
        #                         },
        #                         {
        #                             "end": 66768,
        #                             "start": 66349
        #                         },
        #                         {
        #                             "end": 66715,
        #                             "start": 66304
        #                         },
        #                         {
        #                             "end": 66538,
        #                             "start": 66160
        #                         },
        #                         {
        #                             "end": 66903,
        #                             "start": 66484
        #                         },
        #                         {
        #                             "end": 67157,
        #                             "start": 66727
        #                         },
        #                         {
        #                             "end": 66653,
        #                             "start": 66276
        #                         },
        #                         {
        #                             "end": 66893,
        #                             "start": 66445
        #                         },
        #                         {
        #                             "end": 67065,
        #                             "start": 66658
        #                         },
        #                         {
        #                             "end": 66627,
        #                             "start": 66226
        #                         },
        #                         {
        #                             "end": 66598,
        #                             "start": 66180
        #                         },
        #                         {
        #                             "end": 66690,
        #                             "start": 66244
        #                         },
        #                         {
        #                             "end": 66894,
        #                             "start": 66472
        #                         },
        #                         {
        #                             "end": 66860,
        #                             "start": 66441
        #                         },
        #                         {
        #                             "end": 66393,
        #                             "start": 65965
        #                         },
        #                         {
        #                             "end": 66300,
        #                             "start": 65918
        #                         },
        #                         {
        #                             "end": 67260,
        #                             "start": 66829
        #                         },
        #                         {
        #                             "end": 66609,
        #                             "start": 66186
        #                         },
        #                         {
        #                             "end": 66880,
        #                             "start": 66468
        #                         },
        #                         {
        #                             "end": 66241,
        #                             "start": 65818
        #                         },
        #                         {
        #                             "end": 67025,
        #                             "start": 66585
        #                         },
        #                         {
        #                             "end": 66567,
        #                             "start": 66148
        #                         },
        #                         {
        #                             "end": 67026,
        #                             "start": 66606
        #                         },
        #                         {
        #                             "end": 66167,
        #                             "start": 65763
        #                         }
        #                     ]
        #                 },
        #                 "ingested_at": "2025-11-18T19:37:27.745046134+00:00",
        #                 "processed_at": "2025-11-18T19:37:35.723694755+00:00",
        #                 "completed_at": "2025-11-18T19:37:35.852569160+00:00"
        #             }
        #         },
        #         {
        #             "endpoint_name": "now",
        #             "config": {
        #                 "stream": "now"
        #             },
        #             "metrics": {
        #                 "total_bytes": 9800,
        #                 "total_records": 1225,
        #                 "buffered_records": 0,
        #                 "buffered_bytes": 0,
        #                 "num_transport_errors": 0,
        #                 "num_parse_errors": 0,
        #                 "end_of_input": false
        #             },
        #             "fatal_error": null,
        #             "paused": false,
        #             "barrier": false,
        #             "completed_frontier": null
        #         },
        #         {
        #             "endpoint_name": "user_props_input.user_props",
        #             "config": {
        #                 "stream": "user_props_input"
        #             },
        #             "metrics": {
        #                 "total_bytes": 75094730801,
        #                 "total_records": 70015762,
        #                 "buffered_records": 1000042,
        #                 "buffered_bytes": 1105783091,
        #                 "num_transport_errors": 0,
        #                 "num_parse_errors": 0,
        #                 "end_of_input": false
        #             },
        #             "fatal_error": null,
        #             "paused": false,
        #             "barrier": false,
        #             "completed_frontier": {
        #                 "metadata": {
        #                     "offsets": [
        #                         {
        #                             "end": 1409668,
        #                             "start": 1409014
        #                         },
        #                         {
        #                             "end": 965667,
        #                             "start": 965667
        #                         },
        #                         {
        #                             "end": 1544181,
        #                             "start": 1544181
        #                         },
        #                         {
        #                             "end": 1386339,
        #                             "start": 1385698
        #                         },
        #                         {
        #                             "end": 932960,
        #                             "start": 932960
        #                         },
        #                         {
        #                             "end": 1525987,
        #                             "start": 1525987
        #                         },
        #                         {
        #                             "end": 1373430,
        #                             "start": 1372798
        #                         },
        #                         {
        #                             "end": 917784,
        #                             "start": 917784
        #                         },
        #                         {
        #                             "end": 1514604,
        #                             "start": 1514604
        #                         },
        #                         {
        #                             "end": 1364473,
        #                             "start": 1363847
        #                         },
        #                         {
        #                             "end": 906192,
        #                             "start": 906192
        #                         },
        #                         {
        #                             "end": 1505398,
        #                             "start": 1505398
        #                         },
        #                         {
        #                             "end": 1356092,
        #                             "start": 1355471
        #                         },
        #                         {
        #                             "end": 897192,
        #                             "start": 897192
        #                         },
        #                         {
        #                             "end": 1497936,
        #                             "start": 1497936
        #                         },
        #                         {
        #                             "end": 1349725,
        #                             "start": 1349109
        #                         },
        #                         {
        #                             "end": 889438,
        #                             "start": 889438
        #                         },
        #                         {
        #                             "end": 1491940,
        #                             "start": 1491940
        #                         },
        #                         {
        #                             "end": 1343988,
        #                             "start": 1343376
        #                         },
        #                         {
        #                             "end": 883248,
        #                             "start": 883248
        #                         },
        #                         {
        #                             "end": 1486667,
        #                             "start": 1486667
        #                         },
        #                         {
        #                             "end": 1339141,
        #                             "start": 1338532
        #                         },
        #                         {
        #                             "end": 877733,
        #                             "start": 877733
        #                         },
        #                         {
        #                             "end": 1482006,
        #                             "start": 1482006
        #                         },
        #                         {
        #                             "end": 1095124,
        #                             "start": 1095124
        #                         },
        #                         {
        #                             "end": 872255,
        #                             "start": 872255
        #                         },
        #                         {
        #                             "end": 1088834,
        #                             "start": 1088414
        #                         },
        #                         {
        #                             "end": 1071025,
        #                             "start": 1071025
        #                         },
        #                         {
        #                             "end": 867422,
        #                             "start": 867422
        #                         },
        #                         {
        #                             "end": 1060310,
        #                             "start": 1059903
        #                         },
        #                         {
        #                             "end": 1057818,
        #                             "start": 1057818
        #                         },
        #                         {
        #                             "end": 862877,
        #                             "start": 862877
        #                         },
        #                         {
        #                             "end": 1046585,
        #                             "start": 1046187
        #                         },
        #                         {
        #                             "end": 1047389,
        #                             "start": 1047389
        #                         },
        #                         {
        #                             "end": 858805,
        #                             "start": 858805
        #                         },
        #                         {
        #                             "end": 1036102,
        #                             "start": 1035710
        #                         },
        #                         {
        #                             "end": 1039153,
        #                             "start": 1039153
        #                         },
        #                         {
        #                             "end": 855028,
        #                             "start": 855028
        #                         },
        #                         {
        #                             "end": 1027941,
        #                             "start": 1027554
        #                         },
        #                         {
        #                             "end": 1032563,
        #                             "start": 1032563
        #                         },
        #                         {
        #                             "end": 851330,
        #                             "start": 851330
        #                         },
        #                         {
        #                             "end": 1020417,
        #                             "start": 1020034
        #                         },
        #                         {
        #                             "end": 1026979,
        #                             "start": 1026979
        #                         },
        #                         {
        #                             "end": 848063,
        #                             "start": 848063
        #                         },
        #                         {
        #                             "end": 1014396,
        #                             "start": 1014015
        #                         },
        #                         {
        #                             "end": 1021978,
        #                             "start": 1021978
        #                         },
        #                         {
        #                             "end": 844972,
        #                             "start": 844972
        #                         },
        #                         {
        #                             "end": 1009111,
        #                             "start": 1008733
        #                         },
        #                         {
        #                             "end": 1017196,
        #                             "start": 1017196
        #                         },
        #                         {
        #                             "end": 842198,
        #                             "start": 842198
        #                         },
        #                         {
        #                             "end": 1003717,
        #                             "start": 1003341
        #                         },
        #                         {
        #                             "end": 1012690,
        #                             "start": 1012690
        #                         },
        #                         {
        #                             "end": 839474,
        #                             "start": 839474
        #                         },
        #                         {
        #                             "end": 999420,
        #                             "start": 999048
        #                         },
        #                         {
        #                             "end": 1008404,
        #                             "start": 1008404
        #                         },
        #                         {
        #                             "end": 836753,
        #                             "start": 836753
        #                         },
        #                         {
        #                             "end": 995560,
        #                             "start": 995190
        #                         },
        #                         {
        #                             "end": 1005048,
        #                             "start": 1005048
        #                         },
        #                         {
        #                             "end": 834249,
        #                             "start": 834249
        #                         },
        #                         {
        #                             "end": 991961,
        #                             "start": 991593
        #                         },
        #                         {
        #                             "end": 1001675,
        #                             "start": 1001675
        #                         },
        #                         {
        #                             "end": 831878,
        #                             "start": 831878
        #                         },
        #                         {
        #                             "end": 988140,
        #                             "start": 987774
        #                         },
        #                         {
        #                             "end": 999088,
        #                             "start": 999088
        #                         }
        #                     ]
        #                 },
        #                 "ingested_at": "2025-11-18T19:33:41.868679260+00:00",
        #                 "processed_at": "2025-11-18T19:37:35.723694755+00:00",
        #                 "completed_at": "2025-11-18T19:37:35.852569160+00:00"
        #             }
        #         }
        #     ],
        #     "outputs": [
        #         {
        #             "endpoint_name": "contacts_exist.contacts_exist",
        #             "config": {
        #                 "stream": "contacts_exist"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69005715,
        #                 "transmitted_bytes": 21551158808,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "gmail_contacts.gmail_contacts",
        #             "config": {
        #                 "stream": "gmail_contacts"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 0,
        #                 "transmitted_bytes": 0,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "non_gmail_contacts.non_gmail_contacts",
        #             "config": {
        #                 "stream": "non_gmail_contacts"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69009935,
        #                 "transmitted_bytes": 17660128554,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg12.seg12",
        #             "config": {
        #                 "stream": "seg12"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69012349,
        #                 "transmitted_bytes": 11232329315,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg12a.seg12a",
        #             "config": {
        #                 "stream": "seg12a"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 1751814,
        #                 "transmitted_bytes": 285221876,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg12b.seg12b",
        #             "config": {
        #                 "stream": "seg12b"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69013264,
        #                 "transmitted_bytes": 11225953239,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg12c.seg12c",
        #             "config": {
        #                 "stream": "seg12c"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69009639,
        #                 "transmitted_bytes": 11225362778,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg1_active_emailusers.seg1_active_emailusers",
        #             "config": {
        #                 "stream": "seg1_active_emailusers"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69005659,
        #                 "transmitted_bytes": 11232871089,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg1_active_high_value_users.seg1_active_high_value_users",
        #             "config": {
        #                 "stream": "seg1_active_high_value_users"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 0,
        #                 "transmitted_bytes": 0,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg22a.seg22a",
        #             "config": {
        #                 "stream": "seg22a"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 59188504,
        #                 "transmitted_bytes": 9634899800,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg22b.seg22b",
        #             "config": {
        #                 "stream": "seg22b"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69005613,
        #                 "transmitted_bytes": 11232863572,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg22c.seg22c",
        #             "config": {
        #                 "stream": "seg22c"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69011010,
        #                 "transmitted_bytes": 11227219440,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg2a.seg2a",
        #             "config": {
        #                 "stream": "seg2a"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69007398,
        #                 "transmitted_bytes": 11224997721,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg2b.seg2b",
        #             "config": {
        #                 "stream": "seg2b"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 0,
        #                 "transmitted_bytes": 0,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg2c.seg2c",
        #             "config": {
        #                 "stream": "seg2c"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 59188504,
        #                 "transmitted_bytes": 9634899800,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg3.seg3",
        #             "config": {
        #                 "stream": "seg3"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69005628,
        #                 "transmitted_bytes": 11232866015,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg4.seg4",
        #             "config": {
        #                 "stream": "seg4"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 129797,
        #                 "transmitted_bytes": 21136150,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg4b.seg4b",
        #             "config": {
        #                 "stream": "seg4b"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69135886,
        #                 "transmitted_bytes": 11245921233,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg5.seg5",
        #             "config": {
        #                 "stream": "seg5"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 0,
        #                 "transmitted_bytes": 0,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg6.seg6",
        #             "config": {
        #                 "stream": "seg6"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 11643,
        #                 "transmitted_bytes": 1896150,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg_agg1.seg_agg1",
        #             "config": {
        #                 "stream": "seg_agg1"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 0,
        #                 "transmitted_bytes": 0,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg_agg2.seg_agg2",
        #             "config": {
        #                 "stream": "seg_agg2"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 11564,
        #                 "transmitted_bytes": 1883250,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg_behavior.seg_behavior",
        #             "config": {
        #                 "stream": "seg_behavior"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 133449,
        #                 "transmitted_bytes": 10809369,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg_exp_entered_not_exit.seg_exp_entered_not_exit",
        #             "config": {
        #                 "stream": "seg_exp_entered_not_exit"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 152666,
        #                 "transmitted_bytes": 12365946,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg_exp_entered_not_exit_skipped.seg_exp_entered_not_exit_skipped",
        #             "config": {
        #                 "stream": "seg_exp_entered_not_exit_skipped"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 152293,
        #                 "transmitted_bytes": 12335733,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg_multi_evt1.seg_multi_evt1",
        #             "config": {
        #                 "stream": "seg_multi_evt1"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 680,
        #                 "transmitted_bytes": 55080,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg_multi_evt2.seg_multi_evt2",
        #             "config": {
        #                 "stream": "seg_multi_evt2"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 12,
        #                 "transmitted_bytes": 972,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "seg_prof_evt1.seg_prof_evt1",
        #             "config": {
        #                 "stream": "seg_prof_evt1"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 118247,
        #                 "transmitted_bytes": 9578007,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         },
        #         {
        #             "endpoint_name": "user_props_mv.user_props_mv",
        #             "config": {
        #                 "stream": "user_props_mv"
        #             },
        #             "metrics": {
        #                 "transmitted_records": 69014674,
        #                 "transmitted_bytes": 26749819709,
        #                 "queued_records": 0,
        #                 "queued_batches": 0,
        #                 "buffered_records": 0,
        #                 "buffered_batches": 0,
        #                 "num_encode_errors": 0,
        #                 "num_transport_errors": 0,
        #                 "total_processed_input_records": 70613748,
        #                 "memory": 0
        #             },
        #             "fatal_error": null
        #         }
        #     ]
        # }''')
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
        feldera_offsets = {}

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
            logger.error(f"Error extracting Feldera offsets: {e}")
            logger.debug(f"Stats structure: {json.dumps(stats, indent=2)}")
            return {}
    
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
        pipeline_stats: Dict
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
        print(f"Kafka Brokers: {self.kafka_brokers}")
        print(f"Feldera API: {self.feldera_api_url}")
        print("-" * 80)
        
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
            # Fetch Kafka offsets
            kafka_offsets = self.get_kafka_latest_offsets()
            
            # Fetch Feldera pipeline stats
            pipeline_stats = self.get_feldera_pipeline_stats()
            
            # Extract Feldera offsets from stats
            feldera_offsets = self.extract_feldera_offsets(pipeline_stats)
            
            # Calculate lag
            lag = self.calculate_lag(kafka_offsets, feldera_offsets)
            
            # Print comprehensive report
            self.print_report(kafka_offsets, feldera_offsets, lag, pipeline_stats)
            
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
