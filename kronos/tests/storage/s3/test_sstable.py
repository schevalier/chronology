import random
import struct
import sys
import unittest

from boto.s3.connection import S3Connection

from kronos.storage.backends.s3.sstable import create_sstable
from kronos.storage.backends.s3.sstable import SSTable

AWS_ACCESS_KEY_ID = 'AKIAJ3OSEQKRDHH6VLSA'
AWS_SECRET_ACCESS_KEY = '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'

def generate_data(start=0, end=sys.maxint, step=1):
  for i in xrange(start, end, step):
    key = bytearray(struct.pack('@I', i))
    value = '%d:%s' % (i, 'x' * random.randint(0, 50))
    yield key, value

class TestSSTable(unittest.TestCase):
  def setUp(self):
    SSTable.INDEX_BLOCK_SIZE = 1024 # 1kb
    SSTable.SIZE_THRESHOLD = 1024 * 1024 # 1mb
    self.bucket = S3Connection(AWS_ACCESS_KEY_ID,
                               AWS_SECRET_ACCESS_KEY).get_bucket('kronos_test')

  def test_creation(self):
    data = generate_data()
    sstable = create_sstable(self.bucket, 0, 0, data)
    self.assertTrue(sstable.size >= SSTable.SIZE_THRESHOLD)

  def test_overflow(self):
    pass

  def test_scan(self):
    pass

  def test_index(self):
    pass
