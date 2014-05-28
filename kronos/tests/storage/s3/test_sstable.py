import random
import struct
import sys
import unittest

from kronos.storage.s3.sstable import SSTable


def generate_data(start=0, end=sys.maxint, step=1):
  for i in xrange(start, end, step):
    key = bytearray(struct.pack('@I', i))
    value = '%d:%s' % (i, 'x' * random.randint(0, 50))
    yield key, value

class TestSSTable(unittest.TestCase):
  def setUp(self):
    SSTable.INDEX_BLOCK_SIZE = 1024 # 1kb
    SSTable.SIZE_THRESHOLD = 1024 * 1024 # 1mb

  def test_creation(self):
    pass

  def test_overflow(self):
    pass

  def test_scan(self):
    pass

  def test_index(self):
    pass
