# entry point for the program

from gcs_manager import *
from datetime import datetime

my_cred_file = 'wmt-cc-datasphere-prod.json'
bucket_name = 'dev_cucm_cdr_archive'
blob_name: str = 'media'
file_name: str = bucket_name + '_' + str(datetime.now()) + '.txt'

# test for checking connection manager
make_connection = test_authentication(my_cred_file)
print(make_connection)  # should return true in case of a successful connection

# test for creating a bucket
bucket_to_be_created = 'dev_cucm_test_to_be_deleted'
new_bucket = create_bucket_class_location(my_cred_file, bucket_to_be_created)
print(new_bucket) # expected to return the 'bucket_to_be_created' value or error message


# # test for listing down the buckets in the project
buckets = list_buckets(my_cred_file)
print(buckets)  # should return the list of buckets or the error message


# # test for listing sub-dirs in a bucket
subdirs = list_subdirs_in_bucket(my_cred_file, 'cucm_cdr_errors')
print(subdirs)

# test for writing a file to given sub-dir of a bucket
# write_string_to_file(credentials_json, bucket_name, sub_dir, file_name, string)
# print(write_string_to_file(my_cred_file, 'dev_cucm_cdr_archive', 'c23', 'test.txt', 'test'));

# test for deleting an object
# delete_blob(credentials_json, bucket_name, blob_name)
# print(delete_blob(my_cred_file, 'dev_cucm_cdr_archive', 'c23/test.txt'))
