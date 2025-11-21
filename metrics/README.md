# Pipeline Lag Calculator

A comprehensive script to monitor and calculate lag between Kafka topics and Feldera pipeline consumption.

## Features

- ✅ Fetches Feldera pipeline metrics and statistics via REST API
- ✅ Retrieves latest Kafka offsets for all topic partitions
- ✅ Calculates lag between Kafka and Feldera for each partition
- ✅ Displays comprehensive report with:
  - Overall lag metrics
  - Per-topic and per-partition breakdown
  - Pipeline status and performance metrics
  - Consumption rate percentage
- ✅ Supports configuration via file or command-line arguments
- ✅ API key authentication for Feldera

## Installation

First, ensure you have the required dependencies installed:

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

## Configuration

### Option 1: Using config.json (Recommended)

Update `config.json` in the project root with your Feldera settings:

```json
{
  "kafka": {
    "brokers": "localhost:9092",
    "topics": {
      "events": "seg_poc_events",
      "identity": "seg_poc_identity"
    }
  },
  "feldera": {
    "api_url": "https://your-feldera-host.com",
    "pipeline_name": "your_pipeline_name",
    "api_key": "optional_api_key"
  }
}
```

### Option 2: Command-Line Arguments

Override config settings with command-line arguments (see Usage below).

## Usage

### Basic Usage (with config file)

```bash
python metrics/pipeline_lag_calculator.py
```

### With Command-Line Arguments

```bash
# Specify all parameters
python metrics/pipeline_lag_calculator.py \
  --kafka-brokers localhost:9092 \
  --feldera-url https://feldera.example.com \
  --pipeline my_pipeline \
  --topics topic1,topic2

# With API key authentication
python metrics/pipeline_lag_calculator.py \
  --api-key YOUR_API_KEY

# Enable verbose logging
python metrics/pipeline_lag_calculator.py --verbose
```

### Command-Line Options

- `--config PATH`: Path to configuration file (default: `config.json`)
- `--kafka-brokers BROKERS`: Kafka bootstrap servers (overrides config)
- `--feldera-url URL`: Feldera API base URL (overrides config)
- `--pipeline NAME`: Feldera pipeline name (overrides config)
- `--topics TOPICS`: Comma-separated list of topics (overrides config)
- `--api-key KEY`: Feldera API key for authentication
- `--verbose, -v`: Enable verbose debug logging

## Sample Output

```
================================================================================
FELDERA PIPELINE LAG REPORT - 2025-11-18 14:30:45
================================================================================
Pipeline: my_pipeline
Kafka Brokers: localhost:9092
Feldera API: https://feldera.example.com
--------------------------------------------------------------------------------
Pipeline Status: running

OVERALL METRICS:
  Total Kafka Messages: 1,234,567
  Total Feldera Consumed: 1,234,500
  Total Lag: 67
  Consumption Rate: 99.99%

--------------------------------------------------------------------------------
LAG BY TOPIC AND PARTITION:
--------------------------------------------------------------------------------

Topic: seg_poc_events
  Total Lag: 45
  Partition    Kafka Offset       Feldera Offset     Lag         
  ------------ ------------------ ------------------ ------------
  0            500,000            499,980            20
  1            450,000            449,975            25

Topic: seg_poc_identity
  Total Lag: 22
  Partition    Kafka Offset       Feldera Offset     Lag         
  ------------ ------------------ ------------------ ------------
  0            284,567            284,545            22

--------------------------------------------------------------------------------
ADDITIONAL PIPELINE METRICS:
--------------------------------------------------------------------------------
  Total Input Records: 1,234,500
  Total Processed Records: 1,234,500
  Processing Rate: 15,432.50 records/sec
  Uptime: 2h 15m 30s

================================================================================
```

## Feldera API Structure

The script attempts to extract offsets from the Feldera stats API response. The expected structure is:

```json
{
  "status": "running",
  "inputs": {
    "kafka_input": {
      "metrics": {
        "kafka": {
          "offsets": {
            "topic_name": {
              "0": 12345,
              "1": 23456
            }
          }
        }
      }
    }
  }
}
```

If your Feldera API returns a different structure, you may need to modify the `extract_feldera_offsets()` method in the script.

## Troubleshooting

### Issue: "Topic not found"

- Ensure the topic names in your config match the actual Kafka topics
- Verify Kafka broker connectivity

### Issue: "Failed to fetch Feldera stats"

- Check that the Feldera API URL is correct
- Verify the pipeline name exists
- If using authentication, ensure the API key is valid
- Check network connectivity to Feldera host

### Issue: "No Feldera offsets extracted"

- The Feldera API response structure may differ from expected
- Run with `--verbose` flag to see the actual API response
- Modify the `extract_feldera_offsets()` method to match your API structure

### Issue: Large lag values

- This could indicate:
  - Pipeline is processing slower than data is being produced
  - Pipeline was recently started/restarted
  - High load on the Feldera system
  - Resource constraints

## Scheduling with Cron

To monitor lag periodically, add to crontab:

```bash
# Run every 5 minutes
*/5 * * * * cd /path/to/DataGenerator && python metrics/pipeline_lag_calculator.py >> logs/lag.log 2>&1

# Run every hour
0 * * * * cd /path/to/DataGenerator && python metrics/pipeline_lag_calculator.py >> logs/lag.log 2>&1
```

## Integration with Monitoring Systems

The script can be easily integrated with monitoring systems like:

- **Prometheus**: Export metrics in Prometheus format
- **Grafana**: Create dashboards based on log output
- **Alerting**: Parse output and trigger alerts on high lag

Example: Send alert if lag exceeds threshold:

```bash
#!/bin/bash
OUTPUT=$(python metrics/pipeline_lag_calculator.py)
TOTAL_LAG=$(echo "$OUTPUT" | grep "Total Lag:" | awk '{print $3}' | tr -d ',')

if [ "$TOTAL_LAG" -gt 10000 ]; then
    echo "ALERT: High lag detected: $TOTAL_LAG"
    # Send alert via email, Slack, PagerDuty, etc.
fi
```

## Requirements

- Python 3.10+
- kafka-python>=2.0.2
- requests>=2.31.0
- Access to Kafka brokers
- Access to Feldera API endpoint

