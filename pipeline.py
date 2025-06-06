import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions    
import apache_beam.transforms.window as window
import json
import logging
from typing import Any
import yaml
from datetime import datetime, timezone
import argparse
import numpy as np
import decimal

#setup logging
logging.getLogger().setLevel(logging.INFO)

# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [
        "Symbol:INTEGER",
        "Open:NUMERIC",
        "High:NUMERIC",
        "Low:NUMERIC",
        "Close:NUMERIC",
        "Volume:INTEGER",
        "Timestamp:STRING",
        "numTradeTickers:INTEGER",
        "VolumeWeightedPrice:NUMERIC",
        "SMAClose:NUMERIC",
        "AggregateVolume:INTEGER"
    ]
)


def parse_json_message(message: str) -> dict[str, Any]:
    """Parse the input json message"""
    row = json.loads(message)
    logging.info(f"Parsed message: {row}")
    return {
        "Type": row["Type"],
        "Symbol": row["Symbol"],
        "Open": decimal(row["Open"]),
        "High": decimal(row["High"]),
        "Low": decimal(row["Low"]),
        "Close": decimal(row["Close"]),
        "Volume": row["Volume"],
        "Timestamp": row["Timestamp"],
        "numTradeTickers": row["numTradeTickers"],
        "VolumeWeightedPrice": decimal(row["VolumeWeightedPrice"]),

    }

def create_timestamp(message):
    """Parse the timestamp from the message data and use it as the event timestamp for the pipeline"""
    logging.info(f"Creating timestamp for message, timestamp is {message['Timestamp']}")
    yield beam.window.TimestampedValue(message, int(message["Timestamp"]))

def get_latest_by_timestamp(messages, field):
    """Get the latest value of a specified field from a list of messages based on the 'Timestamp' field.
    Args:
        messages: iterable of dicts containing the messages.
        field: the field to extract from the latest message."""
    logging.info(f"Getting latest value for field: {field}")
    latest_msg = max(messages, key=lambda msg: float(msg["Timestamp"]))
    return latest_msg[field]


def run(
    input_subscription: str,
    output_table: str,
    runner: str,
    window_size_sec: int,
    window_period_sec: int,
    beam_args: list[str],
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True, runner=runner)

    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(
                subscription=input_subscription
            ).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(parse_json_message)
            | "Create timestamps" >> beam.ParDo(create_timestamp)
            | "Create sliding window"
            >> beam.WindowInto(window.SlidingWindows(window_size_sec, window_period_sec))
            | "Add Symbol keys" >> beam.WithKeys(lambda msg: msg["Symbol"])
            | "Group by Symbol for ticker specific analytics" >> beam.GroupByKey()
            | "Get statistics"
            >> beam.MapTuple(
                lambda Symbol, messages: {
                    "Symbol": Symbol,
                    "Open": decimal(get_latest_by_timestamp(messages, "Open")), 
                    "High": decimal(get_latest_by_timestamp(messages, "High")),
                    "Low": decimal(get_latest_by_timestamp(messages, "Low")),
                    "Close": decimal(get_latest_by_timestamp(messages, "Close")),
                    "Volume": get_latest_by_timestamp(messages, "Volume"), 
                    "Timestamp": get_latest_by_timestamp(messages, "Timestamp"), 
                    "numTradeTickers": get_latest_by_timestamp(messages, "numTradeTickers"), 
                    "VolumeWeightedPrice": decimal(get_latest_by_timestamp(messages, "VolumeWeightedPrice")), 
                    #need to work out how to get the last value in the window for these fields in a not horrible way
                    "SMAClose": round(decimal(np.average([msg['Close'] for msg in messages])), 4), #calculate sliding window metrics such as moving averages, round to avoid bq insert issues  
                    "AggregateVolume": sum(msg['Volume'] for msg in messages) #calculate total volume for the past window period               
                }
            )
        )

        # Output the results into BigQuery table.
        logging.info(f"Writing to BigQuery table: {output_table}")
        _ = messages | "Write to Big Query" >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    config = yaml.safe_load(open("config.yaml")) #open configuration file


    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        default=
        f"{config['bqProjectId']}.{config['bqDatasetId']}.{config['bqTableId']}", #BQ output table
    )
    parser.add_argument(
        "--input_subscription",
        default=
        f"projects/{config['bqProjectId']}/subscriptions/{config['pubSubSubscription']}", #pubsub input subscription
    )
    parser.add_argument(
        "--runner",
        default=config["runner"],  # Beam runner (e.g., DataflowRunner, DirectRunner)
    )

    args, beam_args = parser.parse_known_args()

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        runner=args.runner,
        window_size_sec= 180,
        window_period_sec= 60,
        beam_args=beam_args
    )