import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions    
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import apache_beam.transforms.window as window
import json
import logging
import os
import yaml
from datetime import datetime, timezone



# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [
        "Type: STRING",
        "Symbol: INTEGER",
        "Open: DECIMAL:",
        "High: DECIMAL",
        "Low: DECIMAL",
        "Close: DECIMAL",
        "Volume: INTEGER",
        "Timestamp: STRING",
        "NumTradeTickers: INTEGER",
        "VolumeWeightedPrice: DECIMAL",
    ]
)


def parse_json_message(message: str) -> dict[str, Any]:
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = json.loads(message)
    return {
        "Type": row["Type"],
        "Symbol": row["Symbol"],
        "Open": row["Open"],
        "High": row["High"],
        "Low": row["Low"],
        "Close": row["Close"],
        "Volume": row["Volume"],
        "Timestamp": print(datetime.fromtimestamp(row['Timestamp'], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")), #conversion from POSIX timestamp to UTC
        "NumTradeTickers": row["NumTradeTickers"],
        "VolumeWeightedPrice": row["VolumeWeightedPrice"],

    }


def run(
    input_subscription: str,
    output_table: str,
    window_interval_sec: int = 60,
    beam_args: list[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(
                subscription=input_subscription
            ).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(parse_json_message)
            | "Fixed-size windows"
            >> beam.WindowInto(window.FixedWindows(window_interval_sec, 0))
        )

        # Output the results into BigQuery table.
        _ = messages | "Write to Big Query" >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    config = yaml.safe_load(open("config.yaml")) #open configuration file


    parser = argparse.ArgumentParser()
    parser.add_argument(
        f"{config["bqProjectId"]}.{config["bqDatasetId"]}.{config["bqTableId"]}", #BQ output table
    )
    parser.add_argument(
        f"projects/{config["bqProjectId"]}/subscriptions/{config["pubSubSubscription"]}",
    )
    parser.add_argument(
        "--window_interval_sec",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    args, beam_args = parser.parse_known_args()

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        window_interval_sec=args.window_interval_sec,
        beam_args=beam_args,
    )