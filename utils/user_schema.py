import fastavro

DEFAULT_MESSAGE_FIELDS = {
    "site_id": "string",
    "bsin": "uuid",
    "user_id": "uuid",
    "email": "email",
    "last_updated": "datetime",
    "contacts": "map<Contact>",
    "properties": {}
}

PROPERTIES = {
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

def generate_user_props_schema(properties_schema=None):
    if properties_schema is None:
        properties_schema = PROPERTIES
        
    # Define complex types
    contact_schema = {
        "type": "record",
        "name": "Contact",
        "fields": [
            {"name": "type", "type": ["null", "string"], "default": None},
            {"name": "value", "type": ["null", "string"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
            {"name": "preferences", "type": ["null", {"type": "array", "items": "string"}], "default": None},
            {"name": "inactivity_reason", "type": ["null", "string"], "default": None},
            {"name": "created_at", "type": ["null", "string"], "default": None},
            {"name": "updated_at", "type": ["null", "string"], "default": None}
        ]
    }
    
    purchase_schema = {
        "type": "record",
        "name": "Purchase",
        "fields": [
            {"name": "datafields", "type": ["null", {
                "type": "record",
                "name": "PurchaseData",
                "fields": [
                    {"name": "total", "type": ["null", "int"], "default": None},
                    {"name": "items", "type": ["null", {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "Item",
                            "fields": [
                                {"name": "item_id", "type": ["null", "int"], "default": None},
                                {"name": "item_name", "type": ["null", "string"], "default": None},
                                {"name": "item_price", "type": ["null", "int"], "default": None}
                            ]
                        }
                    }], "default": None}
                ]
            }], "default": None}
        ]
    }
    
    shopping_cart_schema = {
        "type": "record",
        "name": "ShoppingCart",
        "fields": [
            {"name": "total", "type": ["null", "int"], "default": None}
        ]
    }
    
    # Build properties schema
    prop_fields = []
    defined_custom_types = set()
    
    for prop_name, prop_type in properties_schema.items():
        avro_type = ["null", "string"] # Default
        if prop_type == "bool":
            avro_type = ["null", "boolean"]
        elif prop_type == "custom_struct:purchase":
            if "Purchase" in defined_custom_types:
                avro_type = ["null", "Purchase"]
            else:
                avro_type = ["null", purchase_schema]
                defined_custom_types.add("Purchase")
        elif prop_type == "custom_struct:shopping_cart":
            if "ShoppingCart" in defined_custom_types:
                avro_type = ["null", "ShoppingCart"]
            else:
                avro_type = ["null", shopping_cart_schema]
                defined_custom_types.add("ShoppingCart")
        
        prop_fields.append({"name": prop_name, "type": avro_type, "default": None})
        
    properties_record_schema = {
        "type": "record",
        "name": "Properties",
        "fields": prop_fields
    }
    
    # Build main UserProps schema (content of "insert")
    user_props_fields = []
    for field, field_type in DEFAULT_MESSAGE_FIELDS.items():
        avro_type = ["null", "string"]
        if field == "properties":
            avro_type = ["null", properties_record_schema]
        elif field == "contacts":
            avro_type = ["null", {"type": "map", "values": contact_schema}]
        
        user_props_fields.append({"name": field, "type": avro_type, "default": None})

    # Add additional fields added in generate_user_props
    additional_fields = [
        "replaced_by", "email_md5", "sub_site_ids", "scoped_properties", 
        "scoped_contacts", "imported_from", "consent", "external_ids", 
        "unique_client_ids", "merged_bsins"
    ]
    
    for field in additional_fields:
            if field == "scoped_contacts":
                # Map of Map of Contact
                # Use "Contact" reference since it's already defined in "contacts" field
                avro_type = ["null", {"type": "map", "values": {"type": "map", "values": "Contact"}}]
            elif field in ["sub_site_ids", "unique_client_ids", "merged_bsins"]:
                avro_type = ["null", {"type": "array", "items": "string"}]
            elif field in ["scoped_properties", "imported_from", "consent", "external_ids"]:
                avro_type = ["null", {"type": "map", "values": "string"}] 
            else:
                avro_type = ["null", "string"]
            
            user_props_fields.append({"name": field, "type": avro_type, "default": None})

    user_props_schema = {
        "type": "record",
        "name": "UserProps",
        "fields": user_props_fields
    }
    return user_props_schema

def generate_avro_schema(properties_schema=None):
    
    # Top level schema
    schema = {
        "type": "record",
        "name": "UserRecord",
        "fields": [
            {"name": "insert", "type": generate_user_props_schema(properties_schema)}
        ]
    }
    return schema

