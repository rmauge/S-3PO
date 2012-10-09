#!/usr/bin/env python

"""Manages files on Amazon S3
Usage: When running on multiple nodes it is better to have different 'heart_beat_time_secs' settings or start
the processes at different times. This helps to reduce contention by the nodes.
Run using 'python s3ingest --config ${CONF_FILE_PATH} --node ${NODE}'
Multipart upload based on https://github.com/boto/boto/blob/develop/bin/s3multiput

Requires: Python 2.7
          boto 2.2.0: A Python interface to Amazon Web Services
          Pyinotify 0.9.3: monitor filesystem events with Python under Linux
          filechunkio 1.5

Note: pyinotify 0.9.3 mangles the native logging with its own runtime class.

The 'monitored_directory' configuration item is the local watched directory and maps to the root of the S3 bucket configured as 's3_bucket_name'

Copyright (c) 2012 Sesame Workshop

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

@author: Raymond Mauge
$Date: 2012-10-09 18:15:52 -0400 (Tue, 09 Oct 2012) $
$Revision: 77 $
"""

from Queue import Empty, Queue
from boto.exception import S3ResponseError
from boto.pyami.config import Config
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto import utils
from filechunkio import FileChunkIO
from logging import handlers
from multiprocessing import Pool
from threading import Thread
from time import sleep
import argparse
import fcntl
import logging
import math
import os
import pyinotify
import signal
import stat
import sys
import time
import traceback

#Default filename for the config file
CONFIG_FILE = './s3ingest.conf'
access_key_id = None # needed global because multiprocessing cannot pickle certain objects
secret_access_key = None # needed global because multiprocessing cannot pickle certain objects

# Must be global to be passed around
def upload_progress_cb(bytes_so_far, total_bytes):
    logging.info("{0:d} / {1:d} bytes transferred".format(bytes_so_far, total_bytes))

# Must be global to be passed around
def _upload_part(target_bucket_name, multipart_id, part_num, file_path, offset, bytes, amount_of_retries=10):
    cb = upload_progress_cb
    
    def _upload(retries_left=amount_of_retries):
        try:
            logging.info("Start uploading part #{0:d} of {1}".format(part_num, file_path))
            target_bucket = S3Connection(access_key_id, secret_access_key).get_bucket(target_bucket_name)
            for mp in target_bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(file_path, 'r', offset=offset, bytes=bytes) as fp:
                        hex_digest, base64_digest, data_size = utils.compute_md5(fp, size=bytes)
                        mp.upload_part_from_file(fp=fp, part_num=part_num, cb=cb, num_cb=1,  md5=(hex_digest, base64_digest))
                    break
        except Exception, exc:
            if retries_left:
                _upload(retries_left=retries_left - 1)
            else:
                logging.error("Failed uploading part #{0:d} of {1}".format(part_num, file_path))
                raise exc
        else:
            logging.info("Completed uploading part #{0:d} of {1}".format(part_num, file_path))

    _upload()

class S3Util:
    _AWS_ACCESS_KEY_ID = None
    _AWS_SECRET_ACCESS_KEY = None
    _watch_manager = None
    _watch_descriptor = None
    _notifier = None
    _connection = None
    _watched_dir_offset = None
    _watched_dir = None
    _target_bucket_name = None
    _logger = None
    _queue = Queue()    #Files that are waiting to be uploaded
    _currently_processing = set()   #Files which have been taken off the queue and are being uploaded
    _exit_flag = False
    _active_flag = False
    _file_split_threshold_bytes = 100 * 1024 * 1024 #Max file size bytes before upload is done in separate parts
    _parallel_processes = 2 #Number of processes for uploading parts

    def __init__(self, access_key_id, secret_access_key):
        self._AWS_ACCESS_KEY_ID = access_key_id
        self._AWS_SECRET_ACCESS_KEY = secret_access_key

    def connect(self):
        logging.debug("Connecting to S3")
        self._connection = S3Connection(self._AWS_ACCESS_KEY_ID, self._AWS_SECRET_ACCESS_KEY)
        logging.debug("Connected to S3")
    
    def get_connection(self):
        return S3Connection(self._AWS_ACCESS_KEY_ID, self._AWS_SECRET_ACCESS_KEY)   

    def start_monitoring(self, dir_name):
        self._watched_dir_offset = len(dir_name)
        self._watched_dir = dir_name
        self._watch_manager = pyinotify.WatchManager()
        #IN_CLOSE_WRITE used because it ensures file is completely written to disk before upload begins
        mask = pyinotify.IN_DELETE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_CREATE
        self._notifier = pyinotify.ThreadedNotifier(self._watch_manager, S3Handler(self))
        self._notifier.start()
        self._watch_descriptor = self._watch_manager.add_watch(dir_name, mask, rec=True, auto_add=True)
        logging.debug("Monitoring: {0}".format(dir_name))

    def list_buckets(self):
        bucket_rs = self.get_connection().get_all_buckets()
        for bucket in bucket_rs:
            print "Bucket found: {0}".format(bucket.name)

    def list_keys(self, bucket_name, path, min_size_bytes=0, max_size_bytes=sys.maxint):
        bucket = self.get_connection().get_bucket(bucket_name)
        bucket_list = bucket.list(path)
        print "Keys in bucket {0}, path {1}, greater than {2} bytes and less than {3} bytes".format(bucket_name, path, min_size_bytes, max_size_bytes)
        for key in bucket_list:
            if (key.size >= min_size_bytes ) and (key.size <= max_size_bytes):
                print "{0}: {1} ".format(bucket_name, key.name)
            
    def set_target_bucket_name(self, target_bucket_name):
        self._target_bucket_name = target_bucket_name
        
    def get_target_bucket_name(self):
        return self._target_bucket_name

    def get_target_bucket(self):
        return self.get_connection().get_bucket(self._target_bucket_name)
      
    def get_bucket(self, bucket_name):
        return self.get_connection().get_bucket(bucket_name)
      
    def multipart_upload_file(self, file_path, keyname):
        mp = self.get_target_bucket().initiate_multipart_upload(keyname, headers={}, reduced_redundancy=False)
        
        source_size = os.stat(file_path).st_size
        bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)), 5242880)
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
        
        pool = Pool(processes=self._parallel_processes)
        
        for i in range(chunk_amount):
            offset = i * bytes_per_chunk
            remaining_bytes = source_size - offset
            bytes = min([bytes_per_chunk, remaining_bytes])
            part_num = i + 1
            pool.apply_async(_upload_part, [self.get_target_bucket_name(), mp.id, part_num, file_path, offset, bytes])
        pool.close()
        pool.join()
            
        if len(mp.get_all_parts()) == chunk_amount:
            mp.complete_upload()
            logging.info("Completed upload of {0}".format(file_path))
        else:
            logging.error("Failed upload {0} because parts missing".format(file_path))
            self._currently_processing.discard(file_path)
            mp.cancel_upload()

    def upload_file(self, file_path):
        self._currently_processing.add(file_path)
        key = Key(self.get_target_bucket())
        rel_path = str(file_path[self._watched_dir_offset:])
        key.key = rel_path

        if os.path.isfile(file_path) and os.stat(file_path).st_size > self._file_split_threshold_bytes:
            self.multipart_upload_file(file_path, key.key)
        else:
            fp = open(file_path, "r")
            hex_digest, base64_digest, data_size = utils.compute_md5(fp)
            key.set_contents_from_filename(file_path, cb=upload_progress_cb, num_cb=1, md5=(hex_digest, base64_digest))

        # Check in queue since the same file path may have been added again while this one was uploading    
        if os.path.isfile(file_path) and not self.is_queued(file_path):
            os.remove(file_path)
        self._currently_processing.discard(file_path)

    def get_next(self):
        return self._queue.get(timeout=5)

    def add_to_queue(self, file_path):
        if os.path.isfile(file_path) and not os.path.getsize(file_path) > 0:
            logging.error("Got zero-byte file, {0}, (ignoring)".format(file_path))
            return
        if not self.is_queued(file_path):
            self._queue.put(file_path)

    def task_done(self):
        self._queue.task_done()

    def wait_for_completion(self):
        self._queue.join()

    def is_exit(self):
        return self._exit_flag

    def set_active(self, is_active):
        self._active_flag = is_active

    def is_active(self):
        return self._active_flag

    def is_queued(self, file_path):
        return file_path in self._queue.queue

    def is_currently_processing(self, file_path):
        return file_path in self._currently_processing
    
    def remove_currently_processing(self, file_path):
        self._currently_processing.discard(file_path)

    def signal_handler(self, signal, frame):
        self._exit_flag = True
        logging.debug("Stopping monitors")
        # destroy the inotify's instance on this interrupt (stop monitoring)
        self._watch_manager.rm_watch(self._watch_descriptor.values())
        self._notifier.stop()
        logging.debug("Monitors stopped. Exiting")
        sys.exit(0)

"""Removes filepath items from a queue and begins the upload process to Amazon.

"""
class S3Uploader(Thread):
    def __init__(self, s3_util):
        Thread.__init__(self)
        self.s3_util = s3_util

    def run(self):
        while True:
            if self.s3_util.is_active():
                try:
                    file_path = self.s3_util.get_next()
                    if self.s3_util.is_currently_processing(file_path):
                        #Return removed filepath to queue and continue (needed if same file is sent again)
                        self.s3_util.task_done()
                        self.s3_util.add_to_queue(file_path)
                        continue
                    else:
                        try:
                            logging.info("{0} upload started by thread {1}".format(file_path, self.name))
                            self.s3_util.upload_file(file_path)
                            logging.info("{0} upload completed by thread {1}".format(file_path, self.name))
                        except Exception as e:
                            tb = traceback.format_exc()
                            logging.error("{0} upload failed in thread {1}, error: {2}".format(file_path, self.name, tb))
                            self.s3_util.remove_currently_processing(file_path)
                    self.s3_util.task_done()
                except Empty:
                    #Ignore if queue is empty, just try again
                    pass
                # End if main thread is closing
            if self.s3_util.is_exit():
                return
            sleep(2)

"""Adds filepath items to a queue when the file/dir is fully copied to the filesystem.
"""
class S3Handler(pyinotify.ProcessEvent):
    _s3_util = None

    def __init__(self, s3_util):
        self._s3_util = s3_util

    def process_IN_CLOSE_WRITE(self, event):
        # Create files this way since this ensures that the entire file is written before starting transfer
        file_path = os.path.join(event.path, event.name)
        logging.debug("{0} close_write event received, adding to queue".format(file_path))
        self._s3_util.add_to_queue(file_path)

    def process_IN_CREATE(self, event):
        # Only create directories this way
        try:
            if event.is_dir:
                #file_path = os.path.join(event.path, event.name)
                self._s3_util.add_to_queue(event.path)
        except AttributeError:
            pass
            # Ignore since most events would be files, so hasattr(event, 'is_dir') would be slow

    def process_IN_DELETE(self, event):
        pass
        #print "\nRemoved: {0}".format(os.path.join(event.path, event.name))

def main(argv):
    parser = argparse.ArgumentParser(description='Upload assets to Amazon')
    parser.add_argument('--config',
                        dest='config_filename',
                        action='store',
                        default=CONFIG_FILE,
                        help='optional custom configuration filename')
    parser.add_argument('--node',
                        dest='node_name_override',
                        action='store',
                        default=False,
                        help='optional override for the pid-id specified in the config file')
    parameters = parser.parse_args()

    current_defaults_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), parameters.config_filename)
    config = Config(path=current_defaults_filename)
    global access_key_id
    global secret_access_key
    access_key_id = config.get('Amazon', 'aws_access_key_id')
    secret_access_key = config.get('Amazon', 'aws_secret_access_key')
    log_file_path = config.get('General', 'log_file_path', '/var/log/s3ingest.log')
    log_level = config.getint('General', 'log_level', 20)
    target_bucket_name = config.get('Amazon', 's3_bucket_name')
    monitored_dir_name = config.get('General', 'monitored_directory')
    worker_threads = config.getint('General', 'worker_threads', 5)
    pid_file_path = config.get('General', 'pid_file_path', './s3ingest.semaphore')
    if not parameters.node_name_override:
        pid_id = config.get('General', 'pid_id').rstrip()
    else:
        pid_id = parameters.node_name_override.rstrip()
    HEART_BEAT_TIME_SECS = config.getint('General', 'heart_beat_time_secs', 300)
    MIN_MODIFIED_INTERVAL_SECS = 3600 # 3600 secs = 1 hr. Keep high to allow time for large files to upload and reduce false positives

    if not os.path.exists(monitored_dir_name):
        print "The directory to be monitored '{0}' does not exist".format(monitored_dir_name)
        sys.exit(1)

    logging.basicConfig(filename=log_file_path, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=log_level)
    mailhost = config.get('Mail', 'mailhost')
    fromaddr = config.get('Mail', 'fromaddr')
    toaddrs = config.get('Mail', 'toaddrs')
    smtp_handler = handlers.SMTPHandler(mailhost, fromaddr, toaddrs, 'S3Util error occurred')
    smtp_handler.setLevel(logging.ERROR)
    logging.getLogger().addHandler(smtp_handler)
    
    s3_util = S3Util(access_key_id, secret_access_key)

    s3_util.set_target_bucket_name(target_bucket_name)
    signal.signal(signal.SIGINT, s3_util.signal_handler)
    signal.signal(signal.SIGTERM, s3_util.signal_handler)

    # Check for pid file and create if not found
    if not os.path.exists(pid_file_path):
        pid_file = open(pid_file_path, "w+")
        fcntl.flock(pid_file.fileno(), fcntl.LOCK_EX)
        pid_file.write(str(pid_id))
        fcntl.flock(pid_file.fileno(), fcntl.LOCK_UN)
        pid_file.close()

    s3_util.start_monitoring(monitored_dir_name)

    logging.debug("Starting worker threads")
    for i in range(worker_threads):
        t = S3Uploader(s3_util)
        t.setDaemon(True)
        t.start()

    logging.debug("Worker threads started")

    while True:
        pid_file = open(pid_file_path, "r+")
        logging.debug("Waiting for lock")
        fcntl.flock(pid_file.fileno(), fcntl.LOCK_SH)
        logging.debug("Acquired lock")
        current_pid = pid_file.readline().rstrip()
        st = os.stat(pid_file_path)
        now = time.time()
        pid_modified_time = st[stat.ST_MTIME]
        logging.debug("pid file: {0}, current_host: {1}".format(current_pid, pid_id))
        if pid_id == current_pid:
            logging.debug("State - Active")
            os.utime(pid_file_path, None)
            s3_util.set_active(True)
            # Find files have been unmodified for a defined threshold and assume that they need to be queued
            for dirpath, dirnames, filenames in os.walk(monitored_dir_name):
                for name in filenames:
                    file_path = os.path.normpath(os.path.join(dirpath, name))
                    last_modifed_time = os.path.getmtime(file_path)
                    if ((now - last_modifed_time) > MIN_MODIFIED_INTERVAL_SECS and not
                        (s3_util.is_queued(file_path) or s3_util.is_currently_processing(file_path))):
                        logging.info("Directory scan found file '{0}' older than {1} seconds and added to queue".format(file_path, (now - last_modifed_time)))
                        s3_util.add_to_queue(file_path)
        else:
            if now - pid_modified_time > HEART_BEAT_TIME_SECS:
                logging.debug("Stale pid file found, setting state - Active")
                pid_file.truncate(0)
                pid_file.seek(0)
                pid_file.write(str(pid_id))
                s3_util.set_active(True)
            else:
                logging.debug("State - Inactive")
                s3_util.set_active(False)
        fcntl.flock(pid_file.fileno(), fcntl.LOCK_UN)
        logging.debug("Released lock")
        pid_file.close()
        #Play nice
        sleep(5)

    s3_util.wait_for_completion()
    logging.debug("Exiting")
    sys.exit(0)

if __name__ == "__main__":
    main(sys.argv)