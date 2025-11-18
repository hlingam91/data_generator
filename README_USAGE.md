# User Properties Generator - Conditional Fields

This program generates synthetic user property data and tracks how many messages meet specified conditional criteria.

## Key Features

✅ **Conditional Field Generation**: Define conditions that user properties should match
✅ **Controlled Generation**: Specify exactly how many messages should meet conditions (true vs false)
✅ **Tracking & Reporting**: Automatically tracks and reports message counts at completion

## Installation

```bash
# Install dependencies (if not already done)
uv sync
# OR
pip install -r requirements.txt
```

## Command Line Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--total-messages` | int | 100 | Total number of messages to generate |
| `--target-true-count` | int | 50 | Number of messages where condition should be TRUE |
| `--kafka-broker` | str | localhost:9092 | Kafka broker address |
| `--kafka-topic` | str | identity_events | Kafka topic name |
| `--conditional-fields` | str | (default) | JSON string of conditional fields |

## Usage Examples

### Example 1: Generate 100 messages, 70 with condition TRUE

```bash
python3 user_props_generator.py \
  --total-messages 100 \
  --target-true-count 70
```

### Example 2: Generate 500 messages, 200 with condition TRUE

```bash
python3 user_props_generator.py \
  --total-messages 500 \
  --target-true-count 200 \
  --kafka-broker localhost:9092 \
  --kafka-topic my_topic
```

### Example 3: Custom conditional fields

```bash
python3 user_props_generator.py \
  --total-messages 1000 \
  --target-true-count 600 \
  --conditional-fields '["and", ["eq", "user.all_emails", "true"], ["withinlast", "user.signed_up_at", "30", "days"]]'
```

## Default Conditional Fields

The default condition checks:
- `user.all_emails == "true"` **AND**
- `user.signed_up_at` is within the last 30 days

```python
CONDITIONAL_FIELDS = [
    "and", 
    ["eq", "user.all_emails", "true"], 
    ["withinlast", "user.signed_up_at", "30", "days"]
]
```

## Output Report

At the end of generation, you'll see a report like:

```
==================================================
MESSAGE GENERATION REPORT
==================================================
Total Messages Generated: 100
Messages with Condition TRUE: 70
Messages with Condition FALSE: 30
Target True Count: 70
Actual Match Rate: 70.00%
==================================================
```

## How It Works

1. **Initialization**: Program parses conditional fields and sets target counts
2. **Generation Loop**: 
   - For each message, determines if it should match the condition
   - Generates field values accordingly (e.g., sets `all_emails="true"` for matches)
   - Evaluates the generated message to verify condition
   - Tracks true/false counts
3. **Reporting**: Displays final statistics

## Conditional Field Syntax

### Equality Check
```python
["eq", "user.field_name", "value"]
```

### Date Within Last N Days
```python
["withinlast", "user.date_field", "30", "days"]
```

### AND Logic
```python
["and", condition1, condition2, ...]
```

### OR Logic
```python
["or", condition1, condition2, ...]
```

## Supported Field Types

- `string`: Company, email, city, state, etc.
- `uuid`: Unique identifiers
- `email`: Email addresses
- `datetime`: Timestamps
- `bool_str`: Boolean values as strings ("true"/"false")
- `map<int>`: Nested objects with integer values
- `map<Contact>`: Contact information objects

## Notes

- The generator ensures generated messages match the specified condition distribution
- Messages are sent to Kafka with a 0.1s delay between sends
- All generated data is synthetic using the Faker library



