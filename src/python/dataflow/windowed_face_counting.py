from __future__ import absolute_import

import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterAny, AfterCount, AfterProcessingTime, AccumulationMode, \
    AfterAll

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
    '/home/stolser/Documents/TechStuff/The_Cloud/GCP/my_gcp_credentials/dataflow-wordcount-sa/'
    'professionaldataengineercourse-cd994c8df1d9.json')
bq_faces_windowed_table_name = "image_vision_api_info.faces_by_emotion_window"
bq_faces_windowed_table_schema = [{"mode": "REQUIRED", "name": "window_start", "type": "TIMESTAMP"},
                                  {"mode": "REQUIRED", "name": "window_end", "type": "TIMESTAMP"},
                                  {"mode": "REQUIRED", "name": "emotion", "type": "STRING"},
                                  {"mode": "REQUIRED", "name": "count", "type": "INTEGER"}, {
                                      "fields": [
                                          {"mode": "NULLABLE", "name": "face_detection_confidence", "type": "FLOAT"},
                                          {"mode": "NULLABLE", "name": "emotion", "type": "STRING"},
                                          {"mode": "NULLABLE", "name": "likelihood", "type": "INTEGER"},
                                          {"mode": "NULLABLE", "name": "file_path", "type": "STRING"},
                                          {"mode": "NULLABLE", "name": "ts_seconds", "type": "INTEGER"}],
                                      "mode": "REPEATED", "name": "faces", "type": "RECORD"}]


def get_pipeline_options() -> PipelineOptions:
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'professionaldataengineercourse'
    # Cannot explicitly specify 'service_account_email' and 'GOOGLE_APPLICATION_CREDENTIALS' at the same time.
    # google_cloud_options.service_account_email = ('dataflow-wordcount-sa'
    #                                               '@professionaldataengineercourse.iam.gserviceaccount.com')
    google_cloud_options.region = 'europe-west4'
    google_cloud_options.job_name = 'beamwordcountminimal2'
    google_cloud_options.staging_location = 'gs://beam_wordcount/staging'
    google_cloud_options.temp_location = 'gs://beam_wordcount/temp'
    # google_cloud_options.enable_streaming_engine = True

    options.view_as(DebugOptions).experiments = ['allow_non_updatable_job']

    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'
    standard_options.streaming = True
    return options


def log_p_collection(p_collection, p_collection_name: str):
    label = "Log {}".format(p_collection_name)

    p_collection | label >> beam.Map(
        lambda item: logging.info('%s = %s', p_collection_name, item))


def filter_high_face_confidence(face_info, confidence_threshold):
    logging.info('face_info = %s', face_info)
    return face_info.face_detection_confidence >= confidence_threshold


class FormatFaceInfoPerWindow(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        ts_format = '%Y-%m-%d %H:%M:%S UTC'
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        (emotion, faces) = element

        return [{
            "window_start": window_start,
            "window_end": window_end,
            "emotion": emotion,
            "count": len(faces),
            "faces": faces
        }]


class FilterHighConfidenceFacesDoFn(beam.DoFn):
    def process(self, element):
        if element['face_detection_confidence'] >= 0.7:
            yield element
        else:
            return  # Return nothing


def get_faces_with_high_emotion_likelihood(face_info):
    emotions = ['joy', 'surprise', 'anger', 'sorrow']
    likelihood_threshold = 3  # out of 5. The higher value the higher likelihood.
    faces_with_high_emotion_likelihood = []

    for emotion in emotions:
        emotion_likelihood_name = '{}_likelihood'.format(emotion)
        this_emotion_likelihood = face_info[emotion_likelihood_name]

        if this_emotion_likelihood >= likelihood_threshold:
            faces_with_high_emotion_likelihood.append({
                "face_detection_confidence": face_info['face_detection_confidence'],
                "emotion": emotion,
                "likelihood": this_emotion_likelihood,
                "file_path": face_info['file_path'],
                "ts_seconds": int(face_info['upload_ts'])
            })

    logging.info('faces_with_high_emotion_likelihood = %s', faces_with_high_emotion_likelihood)

    return faces_with_high_emotion_likelihood


def decode_message(message):
    return message.decode('utf-8')


def parse_jsons(json_message):
    # logging.info('json_message before parsing = %s', json_message)
    face_info = json.loads(json_message)

    return face_info


def run(argv=None):
    from apache_beam.transforms.window import TimestampedValue, FixedWindows

    pubsub_input_topic = 'projects/professionaldataengineercourse/topics/faces_on_images'

    with beam.Pipeline(options=get_pipeline_options()) as pipeline:
        logging.info("pipeline_options.input_topic = {}".format(pubsub_input_topic))

        json_messages = \
            (pipeline
             | 'ReadFromPubSubTopic' >> beam.io.ReadFromPubSub(topic=pubsub_input_topic).with_output_types(bytes)
             | 'DecodeMessagesFromPubSub' >> beam.Map(decode_message)
             )

        window_size_s = 30
        allowed_lateness_s = 60
        high_confidence_faces_grouped_by_emotion_count_per_window = (
                json_messages
                | 'ParseJsonMessage' >> beam.Map(parse_jsons)
                | 'FilterHighFaceConfidence' >> beam.ParDo(FilterHighConfidenceFacesDoFn())
                | 'FlatMapFAcesWithHighEmotionLikelihood' >> beam.FlatMap(get_faces_with_high_emotion_likelihood)
                | 'UseCustomTimestamp' >> beam.Map(lambda face_info:
                                                   TimestampedValue(face_info, face_info['ts_seconds']))
                | 'WindowFaceInfo' >> beam.WindowInto(
                        FixedWindows(window_size_s, 0),
                        trigger=AfterWatermark(
                            early=AfterAny(AfterCount(3), AfterProcessingTime(10)),
                            late=AfterAll(AfterCount(2), AfterProcessingTime(20))),
                        allowed_lateness=allowed_lateness_s,
                        accumulation_mode=AccumulationMode.DISCARDING)
                | 'PairEmotionWithFace' >> beam.Map(lambda face_info: (face_info['emotion'], face_info))
                | 'GroupByEmotion' >> beam.GroupByKey()
                | 'FormatOutputForBigQuery' >> beam.ParDo(FormatFaceInfoPerWindow())
        )

        log_p_collection(high_confidence_faces_grouped_by_emotion_count_per_window, "OutputToBigQuery")

        high_confidence_faces_grouped_by_emotion_count_per_window | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            bq_faces_windowed_table_name,
            schema={"fields": bq_faces_windowed_table_schema},
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

        pipeline_result = pipeline.run()
        pipeline_result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
