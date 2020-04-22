from __future__ import absolute_import

import os
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
    '/home/stolser/Documents/TechStuff/The_Cloud/GCP/my_gcp_credentials/dataflow-wordcount-sa/'
    'professionaldataengineercourse-cd994c8df1d9.json')


def get_pipeline_options() -> PipelineOptions:
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'professionaldataengineercourse'
    # Cannot explicitly specify 'service_account_email' and 'GOOGLE_APPLICATION_CREDENTIALS' at the same time.
    google_cloud_options.service_account_email = ('dataflow-wordcount-sa'
                                                  '@professionaldataengineercourse.iam.gserviceaccount.com')
    # google_cloud_options.service_account_email = ('service-124774031323@
    # dataflow-service-producer-prod.iam.gserviceaccount.com')
    google_cloud_options.region = 'europe-west4'
    google_cloud_options.job_name = 'beamwordcountminimal2'
    google_cloud_options.staging_location = 'gs://beam_wordcount/staging'
    google_cloud_options.temp_location = 'gs://beam_wordcount/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    return options


# input_file_path = 'gs://dataflow-samples/shakespeare/kinglear.txt'
input_file_path = 'gs://beam_wordcount/input_data/changelog.txt'

with beam.Pipeline(options=get_pipeline_options()) as pipeline:
    lines = pipeline | "ReadFromFile" >> ReadFromText(input_file_path)
    # lines = pipeline.apply(ReadFromText(input_file_path), "ReadFromFile")

    words = lines | "ExtractWords" >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))

    word_count_pairs = words | "CountWords" >> beam.combiners.Count.PerElement()

    formatted_strings = word_count_pairs | 'MapWordCountToString' >> beam.MapTuple(
        lambda word, count: '%s: %s' % (word, count))

    formatted_strings | "WriteResultToFile" >> beam.io.WriteToText('gs://beam_wordcount/output/counts.txt')

    pipeline_result = pipeline.run()
    pipeline_result.wait_until_finish()
