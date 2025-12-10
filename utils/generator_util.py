import logging
import random
import time
from datetime import datetime, timedelta
from faker import Faker

logger = logging.getLogger(__name__)

# Pre-calculate common values/ranges
START_DATE_TIMESTAMP = int((datetime.now() - timedelta(days=150)).timestamp())
END_DATE_TIMESTAMP = int((datetime.now() + timedelta(days=150)).timestamp())
DATE_RANGE = END_DATE_TIMESTAMP - START_DATE_TIMESTAMP

# Custom struct generators registry
SHOPPING_ITEMS = [
    "phone", "laptop", "tablet", "headphones", "smartwatch", "camera", "speaker",
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
                for _ in range(random.randint(1, 5))
            ]
        }
    },
    "shopping_cart": lambda faker: {
        "total": random.randint(100, 10000)
    }
}

def generate_value(faker, field_name, data_type, bsin_generator_func=None, condition_value=None):
    """Generate mock value based on data type - Optimized"""
    if condition_value is not None:
        return condition_value
    
    # Handle custom struct types
    if data_type.startswith("custom_struct:"):
        struct_type = data_type.split(":", 1)[1]
        generator_func = CUSTOM_STRUCT_GENERATORS.get(struct_type)
        if generator_func:
            return generator_func(faker)
        return {}
    
    if data_type == "string":
        # Fast path for common string fields to avoid expensive Faker calls
        field_lower = field_name.lower()
        if field_lower.endswith("score"):
            return str(random.randint(0, 100))
        elif field_lower.endswith("campaign_name") or field_lower.endswith("campaign"):
            return f"Campaign {random.randint(1, 5)} {random.randint(1000, 9999)}"
        elif field_lower.endswith("url"):
             return f"http://example.com/{random.randint(10000, 99999)}"
        elif field_lower in ("first_name", "name", "last_name", "city", "state", "country"):
            # Fallback to faker for names/places as random strings look bad, 
            # but maybe we can optimize if needed. For now keep faker for these specific ones
            # to maintain some realism, but use fast path for others.
            if field_lower == "first_name": return faker.first_name()
            if field_lower == "last_name": return faker.last_name()
            return faker.word()
        elif field_lower.endswith("email"):
             return f"user{random.randint(1, 1000000)}@example.com"
        
        # Default string fallback
        return "test_string"

    elif data_type == "uuid":
        if field_name == "bsin" and bsin_generator_func:
            return bsin_generator_func()
        return str(random.getrandbits(128)) # Faster than faker.uuid4()
        
    elif data_type == "email":
        return f"user{random.randint(1, 10000000)}@example.com"
        
    elif data_type == "datetime":
        # Fast date generation using timestamp math
        ts = START_DATE_TIMESTAMP + random.randint(0, DATE_RANGE)
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        
    elif data_type == "bool_str":
        return "true" if random.getrandbits(1) else "false"
        
    elif data_type == "bool":
        return bool(random.getrandbits(1))
        
    elif data_type.startswith("map<int>"):
        return {"total": random.randint(100, 10000)}
        
    elif data_type.startswith("map<Contact>"):
        email = f"user{random.randint(1, 1000000)}@example.com"
        ts = START_DATE_TIMESTAMP + random.randint(0, DATE_RANGE)
        time_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return {
            f"email::{email}": {
                "type": "email",
                "value": email,
                "status": "active" if random.getrandbits(1) else "inactive",
                "preferences": ["standard"],
                "inactivity_reason": "unsubscribed" if random.getrandbits(1) else None,
                "created_at": time_str,
                "updated_at": time_str,
            }
        }
    else:
        return data_type

