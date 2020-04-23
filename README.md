## Faces Image recognition on GCP
### Architecture overview
![alt text](readme_images/gcp_pipeline_architecture.jpg)

### Data processing pipeline
1. An image is uploaded to Cloud Storage.
2. A Cloud Function is triggered. The function does the following:
    * uses the Cloud Vision API to recognize faces on the uploaded image
and retrieve info about the possible faces;
    * writes data into a BigQuery table 'images_with_faces' (each image is represented by one row);
    * publish face into into Cloud Pub/Sub: each recognized face is represented by one message;
3. A Dataflow streaming job does the following:
    * reads messages from a Cloud Pub/Sub topic and creates an unbounded PCollection;
    * decode and parse a message from JSON format to a Python dict;
    * filter messages containing faces with high detection_confidence (more than 70%);
    * filter faces only with high emotion_likelihood ('POSSIBLE', 'LIKELY', 'VERY_LIKELY');
    * uses field 'ts_seconds' as a custom timestamp for windowing;
    * performs windowing messages into fixed windows with an AfterWatermark trigger
    for a final pane and a composite trigger (AfterCount(5), AfterProcessingTime(10)) for early firings;
    * groups messages by the 'emotion' field;
    * parses messages into objects of type dict representing the schema of the output BigQuery table;
    * writes data into a BigQuery table 'faces_by_emotion_window';

### Used technologies:
* Python 3.7
* Cloud Storage
* Cloud Functions (Python 3.7)
* Cloud Vision API
* Cloud Pub/Sub
* Cloud Dataflow, Apache Beam (Python SDK 2.20)
* BigQuery

### Cloud Functions
The Cloud Function is deployed from a Cloud Source Repository:
![alt text](readme_images/function_deployment_repo.png)

### Dataflow
![alt text](readme_images/dataflow_job_graph_01.png) ![alt text](readme_images/dataflow_job_graph_02.png)

### BigQuery
BigQuery table 'images_with_faces' results:
![alt text](readme_images/images_with_faces_results.png)
BigQuery table 'faces_by_emotion_window' results:
![alt text](readme_images/faces_by_emotion_window_results.png)