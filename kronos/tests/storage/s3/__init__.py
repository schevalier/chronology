import unittest

from boto.s3.connection import S3Connection

from kronos.conf import settings
from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.storage.s3.record import EventRecord
from kronos.storage.s3.sstable import SSTable
from kronos.utils.math import time_to_kronos_time
from kronos.utils.math import uuid_from_time


INDEX_BLOCK_SIZE = 1024 * 2 # 2kb
MIN_SIZE = 1024 * 1024 * 2 # 2mb
MAX_SIZE = 1024 * 1024 * 4 # 4mb
NUM_EVENTS_PER_SST = 23500 # approximately


class S3TestCase(unittest.TestCase):
  def setUp(self):
    SSTable.INDEX_BLOCK_SIZE = INDEX_BLOCK_SIZE
    SSTable.MIN_SIZE = MIN_SIZE
    SSTable.MAX_SIZE = MAX_SIZE
    self.s3_conn = S3Connection(settings.storage.s3.aws_access_key_id,
                                settings.storage.s3.aws_secret_access_key)
    self.bucket = self.s3_conn.get_bucket(settings.storage.s3.bucket_name)

  def tearDown(self):
    for key in self.bucket.list():
      key.delete()


def generate_records(start_time=10, interval=10, overlapping=1, n=-1,
                     data_generator=lambda e: {}):
  t = start_time
  if n > 0:
    # Make `n` a multiple of `overlapping`, so the terminating condition below
    # is always hit. For negative values of `n` we don't care because the user
    # is responding to decided when to stop getting more records.
    n -= n % overlapping
  while n != 0:
    ids = []
    for _ in xrange(overlapping):
      ids.append(uuid_from_time(t))
    for _id in sorted(ids):
      # Note: The `size` of this EventRecord will always be 552 bytes.
      event = {TIMESTAMP_FIELD: time_to_kronos_time(t),
               ID_FIELD: str(_id)}
      event.update(data_generator(event))
      yield EventRecord(event)
    n -= overlapping
    t += interval
