def list_storage_buckets():
    from google.cloud import storage

    # Explicitly use service account credentials by specifying the private key file.
    storage_client = storage.Client.from_service_account_json(
        '/home/stolser/Documents/TechStuff/The_Cloud/GCP/my_gcp_credentials/dataflow-wordcount-sa/'
        'professionaldataengineercourse-cd994c8df1d9.json')

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print("buckets = {}".format(buckets))


list_storage_buckets()