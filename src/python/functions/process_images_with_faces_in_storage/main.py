def process_images_with_faces_in_storage(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    The project id is expected to be passed into code via
    the env variable "PROJECT_NAME".
    When creating a Cloud Function add a new env variable "PROJECT_NAME"
    and set it to "professionaldataengineercourse"
    """

    import os
    import time
    from google.cloud import vision

    project_id = os.getenv("PROJECT_NAME")
    upload_ts = time.time()
    print("project_id = {}".format(project_id))

    file = event

    bucket_name = "" + file['bucket']
    file_name = "" + file['name']
    file_path = "gs://{}/{}".format(bucket_name, file_name)
    print("Processing file: '{}'.".format(file_path))

    vision_client = vision.ImageAnnotatorClient()

    uploaded_image = vision.types.Image()
    uploaded_image.source.image_uri = file_path

    vision_response = vision_client.face_detection(image=uploaded_image)
    faces = vision_response.face_annotations
    faces_number = len(faces)

    # Names of likelihood from google.cloud.vision.enums
    likelihood_names = ('UNKNOWN', 'VERY_UNLIKELY', 'UNLIKELY', 'POSSIBLE',
                        'LIKELY', 'VERY_LIKELY')

    bq_faces_info = []
    pubsub_faces_info = []

    print('The number of faces in the image: {}'.format(faces_number))
    for face in faces:
        face_detection_confidence = round(face.detection_confidence, 4)
        joy_likelihood = face.joy_likelihood
        surprise_likelihood = face.surprise_likelihood
        anger_likelihood = face.anger_likelihood
        sorrow_likelihood = face.sorrow_likelihood

        bq_faces_info.append({
            "face_detection_confidence": face_detection_confidence,
            "joy_likelihood": joy_likelihood,
            "surprise_likelihood": surprise_likelihood,
            "anger_likelihood": anger_likelihood,
            "sorrow_likelihood": sorrow_likelihood
        })

        pubsub_faces_info.append({
            "face_detection_confidence": face_detection_confidence,
            "joy_likelihood": joy_likelihood,
            "surprise_likelihood": surprise_likelihood,
            "anger_likelihood": anger_likelihood,
            "sorrow_likelihood": sorrow_likelihood,
            "file_path": file_path,
            "upload_ts": upload_ts
        })

    print("pubsub_faces_info = {}".format(pubsub_faces_info))

    insert_data_into_bq(bq_faces_info=bq_faces_info,
                        faces_number=faces_number,
                        file_path=file_path,
                        project_id=project_id,
                        upload_ts=upload_ts)

    publish_face_info_to_pubsub(project_id=project_id,
                                pubsub_faces_info=pubsub_faces_info)


def publish_face_info_to_pubsub(project_id, pubsub_faces_info):
    from google.cloud import pubsub
    import json

    pubsub_publisher = pubsub.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=project_id,
        topic="faces_on_images"
    )

    print("topic_name = {}".format(topic_name))
    print("pubsub_faces_info = {}".format(pubsub_faces_info))

    for face_info in pubsub_faces_info:
        json_face_info = json.dumps(face_info)
        print("json_face_info = {}".format(json_face_info))
        message = bytes(json_face_info, 'utf-8')
        pubsub_publisher.publish(topic_name, message, source='storage')

    print("Sending to Pub/Sub is finished.")


def insert_data_into_bq(bq_faces_info, faces_number, file_path, project_id, upload_ts):
    from google.cloud import bigquery

    bq_client = bigquery.Client()
    # dataset_ref = bq_client.dataset('image_vision_api_info')
    dataset_ref = bigquery.DatasetReference(project_id, "image_vision_api_info")
    table_ref = dataset_ref.table('images_with_faces')
    table = bq_client.get_table(table_ref)  # API call

    bq_rows_to_insert = [{
        "upload_ts": upload_ts,
        "faces_info": bq_faces_info,
        "faces_number": faces_number,
        "file_path": file_path,
    }]

    print("bq_rows_to_insert = {}".format(bq_rows_to_insert))
    errors = bq_client.insert_rows(table, bq_rows_to_insert)
