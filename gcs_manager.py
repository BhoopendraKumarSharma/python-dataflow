from google.cloud import storage
import os, io


def test_authentication(credentials_json):
    """
    :param credentials_json: credentials json file path(can be downloaded from google account)
    :return: if authentication is successful then 1 else error message

    """
    try:
        storage_client = storage.Client.from_service_account_json(credentials_json)
    except FileNotFoundError as e:
        return "FileNotFoundError: No such file or directory: " + credentials_json
    except Exception as u:
        return u
    else:
        return True


def create_bucket_class_location(credentials_json, bucket_name):
    """
    :param credentials_json: credentials_json: credentials json file path(can be downloaded from google account)
    :param bucket_name: name of the bucket
    :return: name of the bucket created
    """

    try:
        storage_client = storage_client = storage.Client.from_service_account_json(credentials_json)

        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        new_bucket = storage_client.create_bucket(bucket, location="us")

        print(
            "Created bucket {} in {} with storage class {}".format(
                new_bucket.name, new_bucket.location, new_bucket.storage_class
            )
        )
    except Exception as u:
        return u
    else:
        return new_bucket


def list_buckets(credentials_json):
    """
    :param credentials_json: credentials json file path(can be downloaded from google account)
    :return: list of buckets in the account
    """
    try:
        storage_client = storage.Client.from_service_account_json(credentials_json)
        bucket_list = list(storage_client.list_buckets())
    except Exception as u:
        return u
    else:
        return bucket_list


# noinspection SpellCheckingInspection
def list_subdirs_in_bucket(credentials_json, bucket_name):
    """
    :param credentials_json: credentials_json: credentials json file path(can be downloaded from google account)
    :param bucket_name: name of the bucket
    :return: list of subdirs and files in them
    """
    try:
        storage_client = storage.Client.from_service_account_json(credentials_json)
        blobs = storage_client.list_blobs(bucket_name)
        subdirs = [blob.name for blob in blobs]
    except Exception as u:
        return u
    else:
        return subdirs


def write_string_to_file(credentials_json, bucket_name, sub_dir, file_name, string):
    """
    :param credentials_json: credentials json file path(can be downloaded from google account)
    :param bucket_name: name of the bucket where the sub_dir is
    :param sub_dir: name of the targeted sub_dir
    :param file_name: filename, file will be created based on datetime stamp
    :param string: in-memory string to be written
    :return: 1 if successful else error description
    """
    file_to_be_created = file_name

    try:
        storage_client = storage.Client.from_service_account_json(credentials_json)
        bucket = storage_client.get_bucket(bucket_name)  # accessing the bucket
        my_string = io.StringIO(string)
        my_file = bucket.blob(sub_dir + '/' + file_to_be_created)
        my_file.upload_from_string(my_string.read(), content_type="text/plain")
        my_string.close()
        blobs = storage_client.list_blobs(bucket)
        for blob in blobs:
            print(blob.name)
    except Exception as u:
        return u
    else:
        return True


def delete_blob(credentials_json, bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    try:
        storage_client = storage.Client.from_service_account_json(credentials_json)

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        msg = f"Blob {blob_name} deleted."

    except Exception as u:
        return u
    else:
        return msg
