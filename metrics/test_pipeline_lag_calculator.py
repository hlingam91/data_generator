import unittest

from metrics.pipeline_lag_calculator import FelderaPipelineLagCalculator


class PipelineLagCalculatorTest(unittest.TestCase):
    def setUp(self):
        self.calculator = FelderaPipelineLagCalculator(
            kafka_brokers="default:9092",
            feldera_api_url="https://feldera.example.com",
            pipeline_name="test",
            topics=[],
        )
        self.pipeline = {
            "program_info": {
                "input_connectors": {
                    "user_props.c1": {
                        "transport": {
                            "name": "kafka_input",
                            "config": {
                                "topic": "identity",
                                "bootstrap.servers": "kafka:9092",
                            },
                        },
                        "paused": False,
                    },
                    "user_props.c3": {
                        "transport": {
                            "name": "kafka_input",
                            "config": {
                                "topic": "identity_delta",
                                "partitions": [3, 7],
                            },
                        },
                        "paused": True,
                    },
                }
            }
        }

    def test_extracts_offsets_per_connector_without_overwriting(self):
        connectors = self.calculator.extract_kafka_connectors(self.pipeline)
        stats = {
            "inputs": [
                {
                    "endpoint_name": "user_props.c1",
                    "completed_frontier": {
                        "metadata": {
                            "offsets": [
                                {"start": 10, "end": 10},
                                {"start": 20, "end": 20},
                            ]
                        }
                    },
                },
                {
                    "endpoint_name": "user_props.c3",
                    "completed_frontier": {
                        "metadata": {
                            "offsets": [
                                {"start": 30, "end": 30},
                                {"start": 40, "end": 40},
                            ]
                        }
                    },
                },
            ]
        }

        offsets = self.calculator.extract_feldera_offsets(stats, connectors)

        self.assertEqual(offsets["user_props.c1"], {0: 10, 1: 20})
        self.assertEqual(offsets["user_props.c3"], {3: 30, 7: 40})

    def test_missing_frontier_is_not_treated_as_zero(self):
        lag = self.calculator.calculate_lag(
            {"user_props.c1": {0: 100}},
            {"user_props.c1": {}},
        )

        self.assertIsNone(lag["user_props.c1"][0])


if __name__ == "__main__":
    unittest.main()
