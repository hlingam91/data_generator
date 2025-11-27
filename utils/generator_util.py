import logging
import random
from faker import Faker

logger = logging.getLogger(__name__)

# Custom struct generators registry
# Add more custom structures here as needed
SHOPPING_ITEMS = [
    "phone", "laptop", "tablet", "headphones", "smartwatch", "camera", "speaker",
    # "keyboard", "mouse", "monitor", "charger", "case", "backpack", "shoes",
    # "jacket", "jeans", "shirt", "dress", "sunglasses", "wallet", "bag",
    # "book", "notebook", "pen", "perfume", "watch", "jewelry", "makeup"
]

CUSTOM_STRUCT_GENERATORS = {
    "purchase": lambda faker: {
        "datafields": {
            "total": random.randint(10, 2000),
            "items": [
                {
                    "item_id": random.randint(1, 100),
                    "item_name": random.choice(SHOPPING_ITEMS),
                    "item_price": random.randint(100, 1000),
                }
                for _ in range(random.randint(1, 5))  # Generate 1-5 items
            ]
        }
    },
    "shopping_cart": lambda faker: {
        "total": random.randint(100, 10000)
    }
}

def generate_value(faker, field_name, data_type, bsin_generator_func=None, condition_value=None):
    """Generate mock value based on data type"""
    # If a specific condition value is provided, use it
    if condition_value is not None:
        return condition_value
    
    # Handle custom struct types
    if data_type.startswith("custom_struct:"):
        struct_type = data_type.split(":", 1)[1]
        generator_func = CUSTOM_STRUCT_GENERATORS.get(struct_type)
        if generator_func:
            return generator_func(faker)
        else:
            logger.warning(f"Unknown custom_struct type: {struct_type}")
            return {}
    
    if data_type == "string":
        if field_name.lower().endswith("email"):
            return faker.email()
        elif field_name.lower().endswith("company"):
            return faker.company()
        elif field_name.lower().endswith("country"):
            return faker.country()
        elif field_name.lower().endswith("city"):
            return faker.city()
        elif field_name.lower().endswith("state"):
            return faker.state()
        elif field_name.lower().endswith("url"):
            return faker.url()
        elif field_name.lower() == "zip":
            return faker.zipcode()
        elif field_name.lower().endswith("campaign_name") or field_name.lower().endswith("campaign"):
            return random.choice(["Campaign 1", "Campaign 2", "Campaign 3", "Campaign 4", "Campaign 5"]) + " " + faker.word()
        elif field_name.lower() == "uid":
            return faker.uuid4() + random.choice(["@icloud.com", "@mac.com", "@gmail.com", "@yahoo.com", "@hotmail.com"])
        elif field_name.lower() == "first_name" or field_name.lower() == "name":
            return faker.first_name()
        elif field_name.lower() == "last_name":
            return faker.last_name()
        elif field_name.lower().endswith("score"):
            return str(random.randint(0, 100))
        return faker.word()
    elif data_type == "uuid":
        # Use BSIN generator if available and field is "bsin"
        if field_name == "bsin" and bsin_generator_func:
            return bsin_generator_func()
        return faker.uuid4()
    elif data_type == "email":
        return faker.email()
    elif data_type == "datetime":
        return faker.date_time_between(start_date='-150d', end_date='+150d').strftime('%Y-%m-%d %H:%M:%S')
    elif data_type == "bool_str":
        return str(random.choice([True, False])).lower()
    elif data_type == "bool":
        return random.choice([True, False])
    elif data_type.startswith("map<int>"):
        return {"total": random.randint(100, 10000)}
    elif data_type.startswith("map<Contact>"):
        # Generate a contact map with email as key
        email = faker.email()
        time = faker.date_time_between(start_date='-30d', end_date='-30d').strftime('%Y-%m-%d %H:%M:%S')
        return {
            
            f"email::{email}": {
                "type": "email",
                "value": email,
                "status": random.choice(["active", "inactive"]),
                "preferences": ["standard"],
                "inactivity_reason": random.choice(["unsubscribed", None]),
                "created_at": time,
                "updated_at": time,
            }
        }
    else:
        # Fixed value case
        return data_type

