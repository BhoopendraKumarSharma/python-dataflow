import time
from apitools.base.py.exceptions import HttpError

max_retries = 5  # set a maximum number of retry attempts
wait_time = 1    # initial wait time between retries, in seconds

for attempt in range(max_retries):
    try:
        # Attempt to write the file
        with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
            if content:
                f.write(content.encode('utf-8'))
            else:
                logging.info(f"No content for file {orig_file_path}. Writing blank file.")

        # If successful, delete the original file
        beam.io.gcsio.GcsIO().delete(orig_file_path)
        break  # exit the loop if successful
    except Exception as e:
        if attempt < max_retries - 1:  # if not the last attempt
            # Wait for the specified amount of time before retrying
            time.sleep(wait_time)
            # Increase the wait time for the next attempt
            wait_time *= 2
        else:
            # If all attempts have failed, log the error
            logging.error(f"Error processing file {orig_file_path} with status {status}: {str(e)}")
