# S-3PO

Drop files on your filesystem and have them automatically uploaded to Amazon S3.
S-3PO listens for files copied to a directory that you choose. The files are uploaded to Amazon mirroring the structure of your local file system.
After upload the files are deleted, leaving only the empty directory structure in place. Errors can be sent to an email address if needed.

## Getting Started
* Get API keys for Amazon S3
* Configure your s3ingest.conf file to point to your directory locally and the remote S3 bucket and any other settings.
* Start S-3PO - 'python s3ingest --config ${CONF_FILE_PATH} --node ${NODE_NAME}'

## Usage

To run on one server only use:

    python s3ingest --config ${CONF_FILE_PATH} --node ${NODENAME}

For redundancy you can run on more than one node using the same command. Only one node will be active at a time.

Make sure that the setting 'pid_file_path' is visible and writable by the nodes, since this is used to coordinate the nodes.
Also set 'pid_id' to be unique for each. This can also be set on the command line using --node ${NODENAME}

The nodes coordinate by setting the 'pid_file_path' to an identifying name and by checking the timestamp for freshness.

## Settings

* The 'heart_beat_time_secs' determines the time before the pid_file is considered stale.
* The 'monitored_directory' is the directory you want watched for any files added to be uploaded to S3.
* The 'worker_threads' determine how many simultaneous uploads are allowed.

## License

This is licensed under the MIT license and is included with the source.
