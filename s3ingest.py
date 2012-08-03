#!/usr/bin/env python

"""Manages files on Amazon S3
Usage: When running on multiple nodes it is better to have different 'heart_beat_time_secs' settings or start
the processes at different times. This helps to reduce contention by the nodes.
Run using 'python s3ingest --config ${CONF_FILE_PATH} --node ${NODE}'

Requires: Python 2.6.5
          boto 2.3.0: A Python interface to Amazon Web Services
          Pyinotify 0.9.3: monitor filesystem events with Python under Linux

Note: pyinotify 0.9.3 mangles the native logging with its own runtime class.

The 'monitored_directory' configuration item is the local watched directory and maps to the root of the S3 bucket configured as 's3_bucket_name'

Copyright (c) 2012 Sesame Workshop

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

@author: Raymond Mauge
$Date: 2012-08-02 18:20:17 -0400 (Thu, 02 Aug 2012) $
$Revision: 66 $
"""

import sys
import os
import signal
import logging
from logging import handlers
import fcntl
import time
import stat
from time import sleep
from threading import Thread
import threading
from Queue import Queue
from Queue import Empty

import boto
from boto.pyami.config import Config
from boto.s3.connection import S3Connection
from boto.s3.key import Key

import pyinotify
import argparse

#Default filename for the config file
CONFIG_FILE = './s3ingest.conf'


class S3Util:
    _AWS_ACCESS_KEY_ID = None
    _AWS_SECRET_ACCESS_KEY = None
    _watch_manager = None
    _watch_descriptor = None
    _notifier = None
    _connection = None
    _watched_dir_offset = None
    _watched_dir = None
    _target_bucket = None
    _logger = None
    _queue = Queue()
    _currently_processing = set()
    _exit_flag = False
    _active_flag = False

    def __init__(self, access_key_id, secret_access_key):
        self._AWS_ACCESS_KEY_ID = access_key_id
        self._AWS_SECRET_ACCESS_KEY = secret_access_key

    def connect(self):
        logging.debug("Connecting to S3")
        self._connection = S3Connection(self._AWS_ACCESS_KEY_ID, self._AWS_SECRET_ACCESS_KEY)
        logging.debug("Connected to S3")

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
        bucket_rs = self._connection.get_all_buckets()
        for bucket in bucket_rs:
            print "Bucket found: {0}".format(bucket.name)

    def list_keys(self, bucket_name, path):
        bucket = self._connection.get_bucket(bucket_name)
        bucket_list = bucket.list(path)
        print "Keys in bucket {0}, path {1}".format(bucket_name, path)
        for key_list in bucket_list:
            key = str(key_list.key)
            print "{0}: {1} ".format(bucket_name, key)

    def set_target_bucket(self, target_bucket_name):
        if self._target_bucket is None:
            self._target_bucket = self._connection.get_bucket(target_bucket_name)

    def upload_file(self, file_path):
        self._currently_processing.add(file_path)
        key = Key(self._target_bucket)
        rel_path = str(file_path[self._watched_dir_offset:])
        key.key = rel_path
        key.set_contents_from_filename(file_path)
        if not os.path.isdir(file_path):
            os.remove(file_path)
        self._currently_processing.discard(file_path)

    def get_next(self):
        return self._queue.get(timeout=5)

    def add_to_queue(self, file_path):
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

    def signal_handler(self, signal, frame):
        self._exit_flag = True
        logging.debug("Stopping monitors")
        # destroy the inotify's instance on this interrupt (stop monitoring)
        self._watch_manager.rm_watch(self._watch_descriptor.values())
        self._notifier.stop()
        logging.debug("Monitors stopped. Exiting")
        sys.exit(0)


class S3Uploader(Thread):
    def __init__(self, s3_util):
        Thread.__init__(self)
        self.s3_util = s3_util

    def run(self):
        while True:
            if self.s3_util.is_active():
                try:
                    file_path = self.s3_util.get_next()
                    try:
                        logging.info("{0} upload started by thread {1}".format(file_path, self.name))
                        self.s3_util.upload_file(file_path)
                        logging.info("{0} upload completed by thread {1}".format(file_path, self.name))
                    except Exception as e:
                        logging.error("{0} upload failed in thread {1}, error: {2}".format(file_path, self.name, e))
                    self.s3_util.task_done()
                except Empty:
                    # End if main thread is closing
                    if self.s3_util.is_exit():
                        return
            sleep(2)


class S3Handler(pyinotify.ProcessEvent):
    _s3_util = None

    def __init__(self, s3_util):
        self._s3_util = s3_util

    def process_IN_CLOSE_WRITE(self, event):
        # Create files this way since this ensures that the entire file is written before transfering
        file_path = os.path.join(event.path, event.name)
        logging.debug("{0} close_write event received".format(file_path))
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
    s3_util.connect()

    s3_util.set_target_bucket(target_bucket_name)
    signal.signal(signal.SIGINT, s3_util.signal_handler)
    signal.signal(signal.SIGTERM, s3_util.signal_handler)

    # Check for pid file and create if not found
    if not os.path.exists(pid_file_path):
        pid_file = open(pid_file_path, "w+")
        fcntl.flock(pid_file.fileno(), fcntl.LOCK_EX)
        pid_file.write(str(pid_id))
        fcntl.flock(pid_file.fileno(), fcntl.LOCK_UN)
        pid_file.close()
        s3_util.set_active(True)

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
        fcntl.flock(pid_file.fileno(), fcntl.LOCK_EX)
        logging.debug("Acquired lock")
        current_pid = pid_file.readline().rstrip()
        st = os.stat(pid_file_path)
        now = time.time()
        modified_time = st[stat.ST_MTIME]
        logging.debug("pid file: {0}, current_host: {1}".format(current_pid, pid_id))
        if pid_id == current_pid:
            logging.debug("State - Active")
            os.utime(pid_file_path, None)
            s3_util.set_active(True)
            # Find files that failed to upload since last check and are not queued or being processed
            for dirpath, dirnames, filenames in os.walk(monitored_dir_name):
                for name in filenames:
                    file_path = os.path.normpath(os.path.join(dirpath, name))
                    if not s3_util.is_currently_processing(file_path) and not s3_util.is_queued(file_path):
                        logging.debug("Directory walk adds {0} to queue", format(file_path))
                        s3_util.add_to_queue(os.path.normpath(file_path))
        else:
            if now - modified_time > HEART_BEAT_TIME_SECS:
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
