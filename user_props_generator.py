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
from utils.config_loader import ConfigLoader
from utils.generator_util import generate_value
import time
import argparse
from threading import Thread, Lock
from queue import Queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



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
        self.flush_interval = kwargs.get("flush_interval", 100)  # Flush every N messages
        
        # Load BSINs from file if provided
        self.bsins = []
        self.bsin_index = 0
        bsin_file = kwargs.get("bsin_file", None)
        if bsin_file:
            self._load_bsins(bsin_file)
    
    def _load_bsins(self, file_path: str):
        """Load BSINs from a file"""
        try:
            with open(file_path, 'r') as f:
                self.bsins = [line.strip() for line in f if line.strip()]
            self.bsin_length = len(self.bsins)
            logger.info(f"Loaded {len(self.bsins)} BSINs from {file_path}")
        except Exception as e:
            logger.error(f"Failed to load BSINs from {file_path}: {e}")
            raise
    
    def _get_next_bsin(self):
        """Get a random BSIN from the loaded list"""
        if not self.bsins:
            return self.faker.uuid4()
        
        return random.choice(self.bsins)

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
        return generate_value(self.faker, field_name, data_type, self._get_next_bsin, condition_value)

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
    # Load configuration
    config = ConfigLoader.load()
    
    parser = argparse.ArgumentParser(description='Generate user properties with conditional fields')
    parser.add_argument('--total-messages', type=int, default=50000000, help='Total number of messages to generate')
    parser.add_argument('--target-true-count', type=int, default=50000000, help='Target number of messages where condition is true')
    parser.add_argument('--kafka-broker', type=str, default=None, help='Kafka broker address (overrides config file)')
    parser.add_argument('--kafka-topic', type=str, default=None, help='Kafka topic name (overrides config file)')
    parser.add_argument('--conditional-fields', type=str, default=None, help='JSON string of conditional fields')
    parser.add_argument('--num-threads', type=int, default=None, help='Number of threads for parallel generation (overrides config file)')
    parser.add_argument('--bsin-file', type=str, default=None, help='Path to file containing BSINs (overrides config file)')
    parser.add_argument('--config', type=str, default=None, help='Path to config file (default: config.json)')
    
    args = parser.parse_args()
    
    # Reload config if custom config file specified
    if args.config:
        config = ConfigLoader.load(args.config)
    
    # Get configuration values with command-line overrides
    kafka_broker = args.kafka_broker or ConfigLoader.get_kafka_brokers()
    kafka_topic = args.kafka_topic or ConfigLoader.get_kafka_topic("identity")
    num_threads = args.num_threads or ConfigLoader.get_default("num_threads", 5)
    bsin_file = args.bsin_file or ConfigLoader.get_bsin_file()
    
    # Parse conditional fields if provided
    conditional_fields = CONDITIONAL_FIELDS
    if args.conditional_fields:
        try:
            conditional_fields = json.loads(args.conditional_fields)
        except json.JSONDecodeError:
            logger.error("Invalid JSON for conditional_fields, using default")
    
    logger.info(f"Using Kafka Broker: {kafka_broker}")
    logger.info(f"Using Kafka Topic: {kafka_topic}")
    logger.info(f"Using {num_threads} threads")
    logger.info(f"Using BSIN file: {bsin_file}")
    
    generator = UserPropsGenerator(
        kafka_broker=kafka_broker,
        kafka_topic=kafka_topic,
        total_messages=args.total_messages,
        target_true_count=args.target_true_count,
        conditional_fields=conditional_fields,
        num_threads=num_threads,
        bsin_file=bsin_file
    )
    generator.run()
