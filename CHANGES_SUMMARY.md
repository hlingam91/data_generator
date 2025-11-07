# Summary of Changes to user_props_generator.py

## Overview
Modified the user properties generator to support conditional field generation with tracking and reporting capabilities.

## Key Changes

### 1. **Added Imports**
- `argparse`: For command-line argument parsing
- `timedelta`: From datetime, for date range calculations

### 2. **Updated Class Initialization**
Added new instance variables to `UserPropsGenerator.__init__()`:
```python
self.conditional_fields = kwargs.get("conditional_fields", CONDITIONAL_FIELDS)
self.total_messages = kwargs.get("total_messages", MESSAGE_COUNT)
self.target_true_count = kwargs.get("target_true_count", 0)
self.true_count = 0  # Tracks messages where condition = TRUE
self.false_count = 0  # Tracks messages where condition = FALSE
```

### 3. **New Method: `_parse_conditions()`**
Parses conditional field expressions into structured format:
- Handles `and`/`or` operators recursively
- Extracts `eq` (equality) conditions
- Extracts `withinlast` (date range) conditions
- Returns structured condition data for evaluation

### 4. **New Method: `_evaluate_condition()`**
Evaluates if a generated message meets the specified conditions:
- Supports nested field paths (e.g., `properties.all_emails`)
- Handles `and`/`or` logic
- Validates equality and date range conditions
- Returns boolean result

### 5. **New Method: `_get_condition_value()`**
Generates field values based on whether condition should match:
- For equality conditions: Returns exact value or opposite
- For date conditions: Generates dates within/outside specified range
- Ensures generated data meets or violates conditions as required

### 6. **Updated Method: `_generate_value()`**
Added `condition_value` parameter:
- If condition_value is provided, uses it instead of random generation
- Ensures fields match conditional requirements

### 7. **Updated Method: `generate_user_props()`**
Added `should_match_condition` parameter:
- Parses conditions before generation
- Determines values for conditional fields
- Generates properties that match or violate conditions as needed

### 8. **Updated Method: `run()`**
Complete rewrite with tracking and reporting:
- Determines for each message whether condition should match
- Generates messages accordingly
- Evaluates and tracks condition results
- Prints detailed report at completion

### 9. **Updated Main Section**
Added argument parser with options:
- `--total-messages`: Total messages to generate
- `--target-true-count`: How many should meet conditions
- `--kafka-broker`: Kafka broker address
- `--kafka-topic`: Topic name
- `--conditional-fields`: Custom condition JSON

### 10. **Fixed CONDITIONAL_FIELDS Format**
Changed from JSON string to Python list for easier parsing:
```python
# Before: "[\"and\",[\"eq\", \"user.all_emails\", \"true\"], ,[\"withinlast\", \"user.signed_up_at\", \"30\", , \"days\"]]"
# After:
CONDITIONAL_FIELDS = ["and", ["eq", "user.all_emails", "true"], ["withinlast", "user.signed_up_at", "30", "days"]]
```

## Report Output Example

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

## Backward Compatibility
- All changes are backward compatible
- Default values maintain original behavior
- Can be used with or without command-line arguments

## Testing
Run with different parameters to verify:
```bash
# Generate 10 messages, 6 with condition TRUE
python3 user_props_generator.py --total-messages 10 --target-true-count 6

# Generate 100 messages, 80 with condition TRUE
python3 user_props_generator.py --total-messages 100 --target-true-count 80
```


