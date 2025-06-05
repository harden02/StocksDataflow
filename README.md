# StocksDataFlow

This is an initial Apache Beam pipeline (intended to run on GCP DataFlow) which recieves the Pub/Sub output from my AlpacaStreaming project (https://github.com/harden02/AlpacaStreaming), 
performs some simple transformations and aggregations on the fly, and then writes the results to GCP BigQuery in a streaming insert. 
The intention is to create a stock streaming pipeline where data is cleaned and aggregations for metrics such as moving averages and other possible indicators are computed on the fly in real time.

## Features

- Recieves input JSON from GCP Pub/Sub
- Clean, transform, and store data in Google Cloud Platform's BigQuery.

This is an early iteration which is being improved with more metrics, different window durations and an overall cleanup of the pipeline's code where possible. Currently I am hosting it on the local Directrunner to keep cloud costs down whilst I build and test new features.

Here is a preliminary design pattern diagram:

![design drawio](https://github.com/user-attachments/assets/22f9d9dd-c819-4de0-aaab-c8bf5f491149)
