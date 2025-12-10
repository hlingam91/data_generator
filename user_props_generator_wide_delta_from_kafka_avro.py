# This script generates synthetic data for user properties and pushes to kafka topic in Avro format.
# Inputs: Custom Fields with datatype n range selection for fields like dates, numbers, strings, booleans, etc. -- pushes to kafka messages in range/out of range
# Output: how many messages in range/out of range were pushed to kafka (Avro serialized)
# Sample messages format (before Avro serialization):
# {"insert": {"site_id": "boomtrain","bsin": "0002ac00-83ce-4071-bcd2-7a1089812b621","user_id": "6900077","email": "","contacts": {},"properties": {  "has_active_email": "true","all_emails": "true", "double_optin": "true", "shopping_cart":{"total": 5000},"signed_up_at": "2025-10-15 23:05:00", "appointment_time": "2025-10-15 05:31:00", "last_purchase": {"datafields": {"total":200, "items": [{"item_id": 1, "item_name": "brush"}]}} },"last_updated": "2025-04-25 0:00:00","replaced_by": "","email_md5": "","sub_site_ids": [],"scoped_properties": {},"scoped_contacts": {},"imported_from": {},"consent": {},"external_ids": {},"unique_client_ids": [],"merged_bsins": []   }}
# {"insert": {"site_id": "boomtrain","bsin": "6test2fecc6aa9-81cc-4370-aba9-a35d95ae4969","user_id": "6900077","email": "","contacts": {"email::hlingam@zetaglobal.com":  { "type": "email","value": "hlingam@zetaglobal.com","status": "inactive","preferences": ["standard"],"inactivity_reason": "unsubscribed","created_at": 1536238279,"updated_at": 1701117523,"last_inactivity_updated_at": 1605806750,"last_clicked": null,"last_opened": null,"last_sent": 1701117491,"last_purchased": null,"domain": "zetaglobal.com","signed_up_at": 1536238279,"double_opt_in_status": null,"country_code": null,"area_code": null,"timezone": null,"geolocation": null,"phone_type": null,"contact_properties": null}}, "properties": { "has_active_email": "true", "shopping_cart":{"total": 10000},"signed_up_at": "2025-10-21 21:00:00","signed_up": "2025-10-21 21:00:00", "appointment_time": "2025-10-15 05:31:00"  },"last_updated": "2025-04-25 0:00:00","replaced_by": "","email_md5": "","sub_site_ids": [],"scoped_properties": {},"scoped_contacts":{},"imported_from": {},"consent": {},"external_ids": {},"unique_client_ids": [],"merged_bsins": []   }}

import json
import logging
import random
from datetime import datetime
from faker import Faker
import io
import fastavro
from kafka import KafkaConsumer
from producers.kafka_producer import KafkaProducer
from utils.config_loader import ConfigLoader
from utils.generator_util import generate_value
from utils.user_schema import generate_avro_schema,generate_user_props_schema
import time
import argparse
from threading import Thread, Lock
from queue import Queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

properties = {
    # Core User Properties
    "has_active_email": "bool",
    "all_emails": "bool",
    "double_optin": "bool",
    "has_active_subscription": "bool",
    "banned": "bool",
    "hard_bounced": "bool",
    "last_purchase": "custom_struct:purchase",
    "shopping_cart": "custom_struct:shopping_cart",
    
    # User Basic Info
    "first_name": "string",
    "last_name": "string",
    "name": "string",
    "email_status": "string",
    "country": "string",
    "acquisition_country": "string",
    "profile_type": "string",
    "signup_type": "string",
    "registration_device": "string",
    "registration_source": "string",
    "registration_redirect": "string",
    "registration_url": "string",
    "registration_subscribe": "string",
    "registration_email_campaign": "string",
    "ns_browser": "string",
    "ns_country": "string",
    "ns_region": "string",
    "z_country": "string",
    "migration_isp": "string",
    "uid": "string",
    "invite_code_external_alpha_testing_one": "string",
    
    # Timestamps
    "signed_up_at": "datetime",
    "appointment_time": "datetime",
    "created_at": "datetime",
    "last_clicked": "datetime",
    "last_opened": "datetime",
    "last_contact": "datetime",
    "last_seen": "datetime",
    "registration_user_score": "datetime",
    
    # Newsletter Subscriptions - Boolean flags
    "adulthood": "bool",
    "arabic_breaking_news": "bool",
    "audio_shows": "bool",
    "breaking_news": "bool",
    "btb": "bool",
    "cillizza_alerts": "bool",
    "china": "bool",
    "coronavirus": "bool",
    "creative_mktg": "bool",
    "cross_sell_newsletter": "bool",
    "eat_med": "bool",
    "esp_breaking_news": "bool",
    "esp_five_things": "bool",
    "esp_travel_mexico": "bool",
    "evening_nl": "bool",
    "evenings_nl": "bool",
    "events_citizen": "bool",
    "fitness": "bool",
    "five_things": "bool",
    "five_things_sunday": "bool",
    "five_things_weekdays": "bool",
    "follow_digest": "bool",
    "gbs": "bool",
    "global_briefing": "bool",
    "good_stuff": "bool",
    "greener": "bool",
    "growth_promo": "bool",
    "health": "bool",
    "keep_watching": "bool",
    "life": "bool",
    "markets_now": "bool",
    "meanwhile": "bool",
    "mideast_nl": "bool",
    "nightcap": "bool",
    "point": "bool",
    "politics_101": "bool",
    "pop_life": "bool",
    "provoke_persuade": "bool",
    "race": "bool",
    "reg_inside": "bool",
    "reg_recs": "bool",
    "reg_welcome": "bool",
    "reg_welcome_follow": "bool",
    "reliable_sources": "bool",
    "royal_news": "bool",
    "science": "bool",
    "sleep": "bool",
    "stress": "bool",
    "ten": "bool",
    "travel_italy": "bool",
    "travel_mexico": "bool",
    "travel_weekly": "bool",
    "uk_live_free": "bool",
    "underscored": "bool",
    "underscored_deals": "bool",
    "vault": "bool",
    "weather_alerts": "bool",
    "what_matters": "bool",
    "wisdom_project": "bool",
    "work_transformed": "bool",
    
    # Newsletter Sources
    "adulthood_source": "string",
    "arabic_breaking_news_source": "string",
    "before_the_bell_source": "string",
    "breaking_news_source": "string",
    "btb_source": "string",
    "china_source": "string",
    "cillizza_alerts_source": "string",
    "coronavirus_source": "string",
    "coronavirus_sunday_source": "string",
    "creative_mktg_source": "string",
    "eat_med_source": "string",
    "esp_breaking_news_source": "string",
    "esp_five_things_source": "string",
    "events_citizen_source": "string",
    "fitness_source": "string",
    "five_things_source": "string",
    "five_things_sunday_source": "string",
    "follow_digest_source": "string",
    "gbs_source": "string",
    "global_briefing_source": "string",
    "good_stuff_source": "string",
    "growth_promo_source": "string",
    "health_source": "string",
    "keep_watching_source": "string",
    "life_source": "string",
    "markets_now_source": "string",
    "meanwhile_source": "string",
    "mideast_nl_source": "string",
    "nightcap_source": "string",
    "paid_sub_prelaunch_source": "string",
    "plus_bookclub_source": "string",
    "plus_prelaunch_source": "string",
    "point_source": "string",
    "pop_life_source": "string",
    "provoke_persuade_source": "string",
    "race_source": "string",
    "reg_inside_source": "string",
    "reg_recs_source": "string",
    "reliable_sources_source": "string",
    "research_source": "string",
    "royal_news_source": "string",
    "science_source": "string",
    "sleep_source": "string",
    "stress_source": "string",
    "ten_source": "string",
    "travel_italy_source": "string",
    "travel_mexico_source": "string",
    "travel_weekly_source": "string",
    "underscored_source": "string",
    "underscored_deals_source": "string",
    "vault_source": "string",
    "weather_alerts_source": "string",
    "what_matters_source": "string",
    "wisdom_project_source": "string",
    "work_transformed_source": "string",
    
    # Newsletter Subscribe Dates
    "adulthood_subscribe": "datetime",
    "arabic_breaking_news_subscribe": "datetime",
    "breaking_news_subscribe": "datetime",
    "btb_subscribe": "datetime",
    "china_subscribe": "datetime",
    "cillizza_alerts_subscribe": "datetime",
    "coronavirus_subscribe": "datetime",
    "coronavirus_sunday_subscribe": "datetime",
    "eat_med_subscribe": "datetime",
    "esp_breaking_news_subscribe": "datetime",
    "esp_cinco_cosas_subscribe": "datetime",
    "esp_five_things_subscribe": "datetime",
    "evening_nl_subscribe": "datetime",
    "events_citizen_subscribe": "datetime",
    "fitness_subscribe": "datetime",
    "five_things_subscribe": "datetime",
    "five_things_sunday_subscribe": "datetime",
    "follow_digest_subscribe": "datetime",
    "gbs_subscribe": "datetime",
    "global_briefing_subscribe": "datetime",
    "good_stuff_subscribe": "datetime",
    "health_subscribe": "datetime",
    "keep_watching_subscribe": "datetime",
    "life_subscribe": "datetime",
    "markets_now_subscribe": "datetime",
    "meanwhile_subscribe": "datetime",
    "mideast_nl_subscribe": "datetime",
    "nightcap_subscribe": "datetime",
    "point_subscribe": "datetime",
    "pop_life_subscribe": "datetime",
    "provoke_persuade_subscribe": "datetime",
    "race_subscribe": "datetime",
    "reg_recs_subscribe": "datetime",
    "reg_welcome_follow_subscribe": "datetime",
    "reliable_sources_subscribe": "datetime",
    "royal_news_subscribe": "datetime",
    "science_subscribe": "datetime",
    "sleep_subscribe": "datetime",
    "stress_subscribe": "datetime",
    "ten_subscribe": "datetime",
    "travel_italy_subscribe": "datetime",
    "travel_mexico_subscribe": "datetime",
    "travel_weekly_subscribe": "datetime",
    "underscored_subscribe": "datetime",
    "underscored_deals_subscribe": "datetime",
    "vault_subscribe": "datetime",
    "weather_alerts_subscribe": "datetime",
    "what_matters_subscribe": "datetime",
    "wisdom_project_subscribe": "datetime",
    "work_transformed_subscribe": "datetime",
    
    # Last Opened Dates
    "breaking_news_last_opened": "datetime",
    "btb_last_opened": "datetime",
    "coronavirus_last_opened": "datetime",
    "eat_med_last_opened": "datetime",
    "esp_breaking_news_last_opened": "datetime",
    "esp_five_things_last_opened": "datetime",
    "evening_nl_last_opened": "datetime",
    "five_things_last_opened": "datetime",
    "five_things_sunday_last_opened": "datetime",
    "follow_digest_last_opened": "datetime",
    "global_briefing_last_opened": "datetime",
    "good_stuff_last_opened": "datetime",
    "health_last_opened": "datetime",
    "keep_watching_last_opened": "datetime",
    "life_last_opened": "datetime",
    "markets_now_last_opened": "datetime",
    "meanwhile_last_opened": "datetime",
    "nightcap_last_opened": "datetime",
    "point_last_opened": "datetime",
    "pop_life_last_opened": "datetime",
    "provoke_persuade_last_opened": "datetime",
    "race_last_opened": "datetime",
    "reg_inside_last_opened": "datetime",
    "reg_recs_last_opened": "datetime",
    "reliable_sources_last_opened": "datetime",
    "science_last_opened": "datetime",
    "ten_last_opened": "datetime",
    "travel_weekly_last_opened": "datetime",
    "underscored_last_opened": "datetime",
    "underscored_deals_last_opened": "datetime",
    "vault_last_opened": "datetime",
    "what_matter_last_opened": "datetime",
    "what_matters_last_opened": "datetime",
    
    # Last Clicked Dates
    "breaking_news_last_clicked": "datetime",
    "btb_last_clicked": "datetime",
    "esp_breaking_news_last_clicked": "datetime",
    "esp_five_things_last_clicked": "datetime",
    "evening_nl_last_clicked": "datetime",
    "five_things_last_clicked": "datetime",
    "five_things_sunday_last_clicked": "datetime",
    "follow_digest_last_clicked": "datetime",
    "good_stuff_last_clicked": "datetime",
    "meanwhile_last_clicked": "datetime",
    "nightcap_last_clicked": "datetime",
    "pop_life_last_clicked": "datetime",
    "reg_recs_last_clicked": "datetime",
    "reliable_sources_last_clicked": "datetime",
    "science_last_clicked": "datetime",
    "travel_weekly_last_clicked": "datetime",
    "underscored_last_clicked": "datetime",
    "underscored_deals_last_clicked": "datetime",
    "what_matters_last_clicked": "datetime",
    
    # Unsubscribe Dates
    "btb_unsubscribe": "datetime",
    "evening_nl_unsubscribe": "datetime",
    "five_things_sunday_unsubscribe": "datetime",
    "five_things_unsubscribe": "datetime",
    "good_stuff_unsubscribe": "datetime",
    "growth_promo_unsubscribe": "datetime",
    
    # Campaign Names
    "adulthood_acquisition_campaign_name": "string",
    "breaking_news_acquisition_campaign_name": "string",
    "btb_acquisition_campaign_name": "string",
    "coronavirus_acquisition_campaign_name": "string",
    "eat_med_acquisition_campaign_name": "string",
    "esp_five_things_acquisition_campaign_name": "string",
    "events_citizen_acquisition_campaign_name": "string",
    "fitness_acquisition_campaign_name": "string",
    "five_things_acquisition_campaign_name": "string",
    "good_stuff_acquisition_campaign_name": "string",
    "health_acquisition_campaign_name": "string",
    "meanwhile_acquisition_campaign_name": "string",
    "mideast_nl_acquisition_campaign_name": "string",
    "nightcap_acquisition_campaign_name": "string",
    "point_acquisition_campaign_name": "string",
    "pop_life_acquisition_campaign_name": "string",
    "race_acquisition_campaign_name": "string",
    "reliable_sources_acquisition_campaign_name": "string",
    "royal_news_acquisition_campaign_name": "string",
    "science_acquisition_campaign_name": "string",
    "sleep_acquisition_campaign_name": "string",
    "ten_acquisition_campaign_name": "string",
    "travel_italy_acquisition_campaign_name": "string",
    "travel_mexico_acquisition_campaign_name": "string",
    "travel_weekly_acquisition_campaign_name": "string",
    "what_matters_acquisition_campaign_name": "string",
    "voter_guide_election_day_campaign": "string",
    "voter_guide_election_lead_campaign": "string",
    "voter_guide_mail_reminder_campaign": "string",
    "voter_guide_reg_reminder_campaign": "string",
    
    # AdSet Names
    "breaking_news_adset_name": "string",
    "coronavirus_adset_name": "string",
    "five_things_adset_name": "string",
    "good_stuff_adset_name": "string",
    "health_adset_name": "string",
    "meanwhile_adset_name": "string",
    "point_adset_name": "string",
    "reliable_sources_adset_name": "string",
    "what_matters_adset_name": "string",
    
    # User Scores
    "arabic_breaking_news_user_score": "string",
    "breaking_news_user_score": "string",
    "btb_user_score": "string",
    "china_user_score": "string",
    "coronavirus_user_score": "string",
    "creative_mktg_user_score": "string",
    "eat_med_user_score": "string",
    "esp_breaking_news_user_score": "string",
    "esp_five_things_user_score": "string",
    "events_citizen_user_score": "string",
    "fitness_user_score": "string",
    "five_things_user_score": "string",
    "five_things_sunday_user_score": "string",
    "global_briefing_user_score": "string",
    "good_stuff_user_score": "string",
    "greener_user_score": "string",
    "health_user_score": "string",
    "keep_watching_user_score": "string",
    "life_user_score": "string",
    "markets_now_user_score": "string",
    "meanwhile_user_score": "string",
    "mideast_nl_user_score": "string",
    "nightcap_user_score": "string",
    "plus_prelaunch_user_score": "string",
    "point_user_score": "string",
    "pop_life_user_score": "string",
    "provoke_persuade_user_score": "string",
    "race_user_score": "string",
    "reliable_sources_user_score": "string",
    "royal_news_user_score": "string",
    "science_user_score": "string",
    "sleep_user_score": "string",
    "stress_user_score": "string",
    "ten_user_score": "string",
    "travel_weekly_user_score": "string",
    "underscored_user_score": "string",
    "vault_user_score": "string",
    "what_matters_user_score": "string",
    "wisdom_project_user_score": "string",
    "work_transformed_user_score": "string",
    
    # Plus Related
    "plus_prelaunch": "bool",
    "plus_prelaunch_research": "bool",
    "plus_holdout_channelwide": "bool",
    "plus_holdout_subonbdg": "bool",
    "plus_recipient_subonbdg": "bool",
    "plus_recipients_breakingnews": "datetime",
    "plus_sub_for_cancel": "bool",
    "paid_sub_prelaunch": "bool",
    
    # Alpha Testing
    "alpha_direct_recruit": "bool",
    "alpha_intercept": "bool",
    "alpha_testing": "bool",
    "adh_preview": "bool",
    "invite_employee_testers": "bool",
    "invite_external_alpha_testing_one": "bool",
    
    # Misc
    "btb_unsub_reason": "string",
    "engagement_politics_view": "string",
    "evening_nl_test_group": "string",
    "five_things_subscription_type": "string",
    "marketing_affiliate_opt_in": "bool",
    "marketing_opt_in": "bool",
    "mrs": "string",
    "session_history": "string",
    "sub": "bool",
    "underscored_suscribe": "datetime",
    "user_activity": "string"
}

class UserPropsGeneratorWide:
    def __init__(self, kafka_broker: str, kafka_topic: str, source_kafka_topic: str, *args, **kwargs):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.source_kafka_topic = source_kafka_topic
        self.args = args
        self.kwargs = kwargs
        self.site = kwargs.get("site", "boomtrain")
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        self.faker = Faker()
        self.properties_schema = kwargs.get("properties", properties)
        self.num_threads = kwargs.get("num_threads", 1)
        self.count_lock = Lock()
        self.start_time = None
        self.end_time = None
        self.flush_interval = kwargs.get("flush_interval", 100)  # Flush every N messages
        
        self.num_fields_to_update = kwargs.get("num_fields_to_update", 2)
        
        # Initialize Kafka Consumer
        try:
            self.consumer = KafkaConsumer(
                self.source_kafka_topic,
                bootstrap_servers=self.kafka_broker,
                auto_offset_reset='earliest',
                group_id=f'user_props_generator_wide_group_{int(time.time())}',  # Unique group ID
                value_deserializer=None,
            )
            logger.info(f"Initialized Kafka Consumer for source topic: {self.source_kafka_topic}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Consumer: {e}")
            raise

        # Initialize Avro schema
        try:
            # Schema for reading (might be flattened if user changed it to generate_user_props_schema)
            self.full_schema = generate_user_props_schema()
            self.parsed_full_schema = fastavro.parse_schema(self.full_schema)
            
            logger.info("Avro Schemas initialized successfully")
            print("Writer Schema (JSON):")
            print(json.dumps(self.full_schema, indent=2))
            
        except Exception as e:
            logger.error(f"Failed to generate schema: {e}")
            raise

        # BSIN list is empty as we don't load from file
        self.bsins = []

    def _get_next_bsin(self):
        """Get the next BSIN from the loaded list (cycles through if needed)"""
        # Since we don't load from file, return random
        return self.faker.uuid4()

    def _generate_value(self, field_name: str, data_type: str):
        """Generate mock value based on data type"""
        return generate_value(self.faker, field_name, data_type, self._get_next_bsin, None)

    def _update_nested_value(self, data, field_path, new_value):
        """Update a value in a nested dictionary using dot notation"""
        keys = field_path.split('.')
        current = data
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            elif not isinstance(current[key], dict):
                return
            current = current[key]
        current[keys[-1]] = new_value

    def _get_field_type(self, field_path):
        """Infer field type from schema or name"""
        if field_path.startswith("properties."):
            prop_name = field_path.split(".", 1)[1]
            if prop_name in self.properties_schema:
                return self.properties_schema[prop_name]
        
        # Fallback based on name
        if "email" in field_path: return "email"
        if "_at" in field_path or "time" in field_path: return "datetime"
        if "bool" in field_path or "is_" in field_path or "has_" in field_path: return "bool"
        return "string"

    def _get_all_property_paths(self):
        """Flatten properties schema into a list of dot-notation paths"""
        paths = []
        for prop, dtype in self.properties_schema.items():
            if dtype.startswith("custom_struct:"):
                pass
                # Keep as is, similar to narrow implementation
            else:
                paths.append(f"properties.{prop}")
        return paths

    def _get_random_fields_to_update(self):
        """Select N random fields to update"""
        all_paths = self._get_all_property_paths()
        if not all_paths:
            return []
        count = min(self.num_fields_to_update, len(all_paths))
        return random.sample(all_paths, count)

    def generate_user_props(self, base_record=None):
        """Generate user properties record based on the fields and datatypes"""
        
        if base_record:
            try:
                record_json = base_record
                # Handle wrapped (insert) or unwrapped
                user_props = record_json.get("insert", record_json)
                
                # Update timestamp
                user_props["last_updated"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Select random fields to update for this record
                fields_to_update = self._get_random_fields_to_update()
                
                for field_path in fields_to_update:
                    field_name = field_path.split('.')[-1]
                    
                    # Determine field type based on path or name
                    field_type = "string" # default
                    if "total" in field_path: field_type = "int"
                    else: field_type = self._get_field_type(field_path)
                    
                    new_value = self._generate_value(field_name, field_type)
                    self._update_nested_value(user_props, field_path, new_value)
                
                return user_props
            except Exception as e:
                logger.error(f"Failed to update record: {e}")
                return None
        
        return None

    def push_to_kafka(self, user_props: dict):
        try:
            # Serialize to Avro
            out_bytes = io.BytesIO()
            fastavro.schemaless_writer(out_bytes, self.parsed_full_schema, user_props)
            
            self.kafka_producer.send(self.kafka_topic, out_bytes.getvalue())
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise

    def _worker_thread(self, message_queue: Queue):
        """Worker thread to generate and push messages"""
        local_count = 0
        
        while True:
            task = message_queue.get()
            if task is None:  # Poison pill
                message_queue.task_done()
                break
            
            try:
                bytes_io = io.BytesIO(task)
                base_record = fastavro.schemaless_reader(bytes_io, self.parsed_full_schema)
            except Exception as e:
                logger.error(f"Failed to decode Avro message: {e}")
                message_queue.task_done()
                continue
            
            user_props = self.generate_user_props(base_record=base_record)
            
            if not user_props:
                message_queue.task_done()
                continue
            
            self.push_to_kafka(user_props)
            # print(user_props)
            
            local_count += 1
            if local_count % self.flush_interval == 0:
                self.kafka_producer.flush()
            
            message_queue.task_done()

    def run(self):
        self.start_time = time.time()
        logger.info(f"Starting message generation: Threads={self.num_threads}")
        logger.info(f"Mode: Delta Updates from Source Kafka Topic {self.source_kafka_topic}")
        logger.info(f"Updating {self.num_fields_to_update} random fields per record")
        logger.info("Starting continuous consumption loop... (Press Ctrl+C to stop)")
        
        message_queue = Queue(maxsize=1000)

        if self.num_threads > 1:
            threads = []
            for _ in range(self.num_threads):
                thread = Thread(target=self._worker_thread, args=(message_queue,))
                thread.start()
                threads.append(thread)
            
            count = 0
            try:
                for message in self.consumer:
                    message_queue.put(message.value)
                    count += 1
                    if count % 1000 == 0:
                        logger.info(f"Queued {count} messages from Kafka")
            except KeyboardInterrupt:
                logger.info("Received shutdown signal (Ctrl+C)")
            except Exception as e:
                logger.error(f"Error during consumption: {e}")
            
            logger.info(f"Consumption stopped. Total messages queued: {count}")
            
            for _ in range(self.num_threads):
                message_queue.put(None)
            
            message_queue.join()
            for thread in threads:
                thread.join()
        else:
            count = 0
            logger.info("Starting consumption from Kafka source (Single Threaded)...")
            try:
                for message in self.consumer:
                    try:
                        message_size = len(message.value)
                        # print(f"Message size: {message_size} bytes")
                        bytes_io = io.BytesIO(message.value)
                        base_record = fastavro.schemaless_reader(bytes_io, self.parsed_full_schema)
                    except Exception as e:
                        logger.error(f"Failed to decode Avro message at offset {message.offset}: {e}")
                        continue

                    user_props = self.generate_user_props(base_record=base_record)
                    if user_props:
                        # print(user_props)
                        self.push_to_kafka(user_props)
                    
                    count += 1
                    if count % self.flush_interval == 0:
                        self.kafka_producer.flush()
                        logger.info(f"Processed {count} messages")
            except KeyboardInterrupt:
                logger.info("Received shutdown signal (Ctrl+C)")
            except Exception as e:
                logger.error(f"Error during consumption: {e}")
        
        self.kafka_producer.flush()
        self.kafka_producer.close()
        self.consumer.close()
        
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        messages_per_sec = count / elapsed_time if elapsed_time > 0 else 0
        
        print("\n" + "="*50)
        print("MESSAGE GENERATION REPORT")
        print("="*50)
        print(f"Total Messages Generated: {count}")
        print(f"Time Taken: {elapsed_time:.2f} seconds")
        print(f"Throughput: {messages_per_sec:.2f} messages/sec")
        print(f"Threads Used: {self.num_threads}")
        print("="*50)


if __name__ == "__main__":
    # Load configuration
    config = ConfigLoader.load()
    
    parser = argparse.ArgumentParser(description='Generate user properties with random updates (Wide Schema)')
    parser.add_argument('--kafka-broker', type=str, default=None, help='Kafka broker address (overrides config file)')
    parser.add_argument('--kafka-topic', type=str, default=None, help='Destination Kafka topic name (overrides config file)')
    parser.add_argument('--source-topic', type=str, default=None, help='Source Kafka topic name (REQUIRED)')
    parser.add_argument('--num_threads', type=int, default=None, help='Number of threads for parallel generation (overrides config file)')
    parser.add_argument('--num-fields', type=int, default=None, help='Number of fields to randomly update per record')
    parser.add_argument('--config', type=str, default=None, help='Path to config file (default: config.json)')
    
    args = parser.parse_args()
    
    if args.config:
        config = ConfigLoader.load(args.config)
    
    kafka_broker = args.kafka_broker or ConfigLoader.get_kafka_brokers()
    kafka_topic = args.kafka_topic or ConfigLoader.get_kafka_topic("identity_delta")
    source_kafka_topic = args.source_topic or ConfigLoader.get_kafka_topic("identity")
    num_threads = args.num_threads or ConfigLoader.get_default("num_threads", 1)
    num_fields_to_update = args.num_fields or 5
    
    logger.info(f"Using Kafka Broker: {kafka_broker}")
    logger.info(f"Destination Topic: {kafka_topic}")
    logger.info(f"Source Topic: {source_kafka_topic}")
    logger.info(f"Using {num_threads} threads")
    
    generator = UserPropsGeneratorWide(
        kafka_broker=kafka_broker,
        kafka_topic=kafka_topic,
        source_kafka_topic=source_kafka_topic,
        num_threads=num_threads,
        num_fields_to_update=num_fields_to_update
    )
    generator.run()
