# | 'CountPerWindow' >> beam.CombineGlobally(sum_values).without_defaults()

# high_face_confidence_count_per_window | "WriteFaceCountsToFile" >> beam.io.WriteToText(
#     'gs://beam_wordcount/output/face_counts')


class FormatAsStringDoFn(beam.DoFn):
    def __init__(self):
        super(FormatAsStringDoFn, self).__init__()

    def process(self, element, window=beam.DoFn.WindowParam):
        ts_format_ = '%Y-%m-%d %H:%M:%S.%f UTC'
        window_start_ = window.start.to_utc_datetime().strftime(ts_format_)
        window_end_ = window.end.to_utc_datetime().strftime(ts_format_)
        return "count = {}; window_start = {}; window_end = {}".format(element, window_start_, window_end_)


# Reading from BigQuery
emotion_likelihood_names = pipeline | 'Reading from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(
    query=("select likelihood_value, likelihood_name from "
           "image_vision_api_info_us.vision_likelihood_names_us;")
)) | 'CollectToList' >> beam.combiners.ToList()

# Joining data
def run_test(argv=None):
    with beam.Pipeline(options=get_pipeline_options()) as pipeline:
        emails_list = [
            ('amy', 'amy@example.com'),
            ('carl', 'carl@example.com'),
            ('julia', 'julia@example.com'),
            ('carl', 'carl@email.com'),
        ]
        phones_list = [
            ('amy', '111-222-3333'),
            ('james', '222-333-4444'),
            ('amy', '333-444-5555'),
            ('carl', '444-555-6666'),
        ]

        emails = pipeline | 'CreateEmails' >> beam.Create(emails_list)
        phones = pipeline | 'CreatePhones' >> beam.Create(phones_list)

        results = ({'emails': emails, 'phones': phones} | beam.CoGroupByKey())
        log_p_collection(results, "CoGroupByKey results")

        pipeline_result = pipeline.run()
        pipeline_result.wait_until_finish()


# ============================================================================
joined_faces = ({'likelihood_to_face_info': likelihood_to_face_info,
                 'emotion_likelihood_names': emotion_likelihood_names}
                | beam.CoGroupByKey()
                | beam.Map(join_info))


def join_info(joined_data):
    (likelihood_value, data) = joined_data
    emotion_likelihood_name = data['emotion_likelihood_names'][0]
    logging.info("join_info: emotion_likelihood_name = {}".format(emotion_likelihood_name))

    likelihood_to_face_info_list = data['likelihood_to_face_info']
    logging.info("join_info: likelihood_to_face_info_list = {}".format(likelihood_to_face_info_list))

    updated_faces = []
    for face in likelihood_to_face_info_list:
        face['likelihood'] = emotion_likelihood_name['likelihood_name']
        updated_faces.append(face)

    logging.info("updated_faces = {}".format(updated_faces))

    return updated_faces
# ============================================================================
