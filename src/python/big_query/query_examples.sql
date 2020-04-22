select STRING(TIMESTAMP_SECONDS(CAST(upload_ts AS INT64)), "+03:00") as upload_time, *
from `image_vision_api_info.images_with_faces` order by upload_ts desc;

select window_start, window_end, emotion, count,
array(select STRING(TIMESTAMP_SECONDS(CAST(ts_seconds AS INT64)), "+03:00") as upload_time from UNNEST(faces)) as faces_formatted
from `image_vision_api_info.faces_by_emotion_window_20` order by window_start desc;

select FORMAT_TIMESTAMP("%c", window_start, "+03:00") as window_start_local,
    FORMAT_TIMESTAMP("%c", window_end, "+03:00") as window_end_local, emotion, count,
    array(select struct(STRING(TIMESTAMP_SECONDS(CAST(ts_seconds AS INT64)), "+03:00") as upload_time,
    face_detection_confidence as face_confidence, likelihood as emotion_likelihood, file_path) from UNNEST(faces)) as faces
from `image_vision_api_info.faces_by_emotion_window` order by window_start desc;

insert into `image_vision_api_info.vision_likelihood_names` (likelihood_value, likelihood_name)
values (0, 'UNKNOWN'), (1, 'VERY_UNLIKELY'), (2, 'UNLIKELY'), (3, 'POSSIBLE'),(4, 'LIKELY'), (5, 'VERY_LIKELY');

select likelihood_value, likelihood_name from `image_vision_api_info.vision_likelihood_names`;

