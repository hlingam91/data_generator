# Configuration Guide

## Overview

The data generators now support configuration via a centralized `config.json` file, which makes it easier to manage Kafka brokers, topics, and other settings.

## Configuration File Location

The default configuration file is `config.json` in the project root directory.

## Configuration Structure

```json
{
  "kafka": {
    "brokers": "broker1:9092,broker2:9092,broker3:9092",
    "topics": {
      "events": "seg_poc_events",
      "identity": "seg_poc_identity"
    }
  },
  "data": {
    "bsin_file": "data/bsins_extracted.txt",
    "users_file": "data/users_extracted.txt"
  },
  "defaults": {
    "site_id": "boomtrain",
    "num_threads": 5,
    "flush_interval": 100
  }
}
```

## Configuration Sections

### kafka
- **brokers**: Comma-separated list of Kafka broker addresses
- **topics**: Named topics for different data streams
  - **events**: Topic for event data
  - **identity**: Topic for user properties/identity data

### data
- **bsin_file**: Path to file containing BSINs (one per line)
- **users_file**: Path to file containing full user records (one JSON per line)

### defaults
- **site_id**: Default site identifier
- **num_threads**: Default number of threads for parallel generation
- **flush_interval**: How often to flush messages to Kafka (in message count)

## Usage

### Using Default Configuration

Both generators will automatically load `config.json`:

```bash
# Events generator
python events_generator.py

# User properties generator
python user_props_generator.py
```

### Overriding Configuration via Command Line

You can override any configuration value using command-line arguments:

```bash
# Override Kafka broker
python events_generator.py --kafka-broker "localhost:9092"

# Override topic
python events_generator.py --kafka-topic "my_custom_topic"

# Override number of threads
python events_generator.py --num-threads 10

# Override BSIN file
python events_generator.py --bsin-file "data/custom_bsins.txt"
```

### Using a Custom Configuration File

You can specify a different config file:

```bash
python events_generator.py --config "config.production.json"
python user_props_generator.py --config "config.staging.json"
```

## Configuration Priority

Configuration values are resolved in the following order (highest priority first):

1. **Command-line arguments** (highest priority)
2. **Configuration file** (config.json or custom file)
3. **Default values** (fallback)

## Benefits

✅ **Centralized Configuration**: Manage all settings in one place  
✅ **Environment Support**: Easy to switch between dev/staging/production  
✅ **Override Flexibility**: Command-line args override config file  
✅ **No Hardcoding**: Kafka brokers are no longer hardcoded in scripts  
✅ **Better Reliability**: Improved flush intervals (100 instead of 1000) for fewer timeouts

## Example: Multiple Environments

Create different config files for each environment:

**config.dev.json**
```json
{
  "kafka": {
    "brokers": "localhost:9092"
  }
}
```

**config.production.json**
```json
{
  "kafka": {
    "brokers": "prod-broker1:9092,prod-broker2:9092,prod-broker3:9092"
  }
}
```

Then run:
```bash
python events_generator.py --config config.dev.json
python events_generator.py --config config.production.json
```

