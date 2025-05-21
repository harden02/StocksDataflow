import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions    
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import json
import logging
import os
import yaml
import retrieveSchema



class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--project', required=True, help='GCP project ID')
        parser.add_value_provider_argument('--input_topic', required=True, help='Pub/Sub input topic')
        parser.add_value_provider_argument('--output_table', required=True, help='BigQuery output table in the format project.dataset.table')
