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
        parser.add_value_provider_argument('--subscriptionName', required=True, help='Pub/Sub subscription name')
        parser.add_value_provider_argument('--outputTable', required=True, help='BigQuery output table in the format project.dataset.table')

def runPipeline(argv=None):

    pipeOptions = PipelineOptions(argv,
                                  runner = 'DataflowRunner',
                                  streaming = True,
                                  save_main_session = True)
    CustomOptions = PipelineOptions.view_as(CustomOptions)

    #retrieve custom options from CLI args
    projectID = CustomOptions.project.get()
    inputSub = CustomOptions.subScriptionName.get()
    outputTable = CustomOptions.outputTable.get()

    #retrieve schema from BigQuery table
    schemaDict = retrieveSchema(projectID, outputTable)

    #simplify schema dict for BQ definition
    schemaDict = {
        key: {
            'type': str(value['type'])
        }
        for key, value in schemaDict.items()
    }


    #define pipeline
    with beam.Pipeline(options=pipeOptions) as p:
        # Read from Pub/Sub
        (p
         | 'ReadFromPubSub' >> ReadFromPubSub(subscription=inputSub)
         | 'ParseJson' >> beam.Map(lambda x: json.loads(x))
         | 'TransformData' >> beam.Map(lambda x: {key: x.get(key, value['default']) for key, value in schemaDict.items()})
         | 'WriteToBigQuery' >> WriteToBigQuery(
             outputTable,
             schema=schemaDict,
             write_disposition=BigQueryDisposition.WRITE_APPEND,
             create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
         )
        )