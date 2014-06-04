import random
import unittest

from boto.s3.connection import S3Connection
from math import ceil

from kronos.conf import settings
from kronos.storage.s3.sstable import create_sstable
from kronos.storage.s3.sstable import COMPRESS_FACTOR
from kronos.storage.s3.sstable import SSTable
from tests.storage.s3 import generate_records

INDEX_BLOCK_SIZE = 1024 * 2 # 2kb
MIN_SIZE = 1024 * 1024 * 2 # 2mb
MAX_SIZE = 1024 * 1024 * 4 # 4mb
NUM_EVENTS_PER_INDEX_BLOCK = int(ceil(INDEX_BLOCK_SIZE /
                                      (405 * COMPRESS_FACTOR))) # = 9
NUM_EVENTS_PER_SST = 23500 # approximately
DIRECTORY = 'kronos_test/'


class TestSSTable(unittest.TestCase):
  def setUp(self):
    SSTable.INDEX_BLOCK_SIZE = INDEX_BLOCK_SIZE
    SSTable.MIN_SIZE = MIN_SIZE
    SSTable.MAX_SIZE = MAX_SIZE
    self.s3_conn = S3Connection(settings.storage.s3.aws_access_key_id,
                                settings.storage.s3.aws_secret_access_key)
    self.bucket = self.s3_conn.get_bucket(settings.storage.s3.bucket_name)

  def tearDown(self):
    for key in self.bucket.list(prefix=DIRECTORY):
      key.delete()

  def test_creation(self):
    # Test without overflow.
    records = generate_records(n=1000)
    sst_key, records = create_sstable(records, self.bucket,
                                      directory=DIRECTORY,
                                      ancestors=['lol'],
                                      siblings=['cat'])
    self.assertTrue(sst_key.size <= SSTable.MIN_SIZE)
    self.assertTrue(self.bucket.get_key(sst_key.name.rstrip('.sst') + '.idx'))
    sst_key = self.bucket.get_key(sst_key.name)
    for key in SSTable.METADATA_KEYS:
      self.assertTrue(sst_key.get_metadata(key) is not None)
    self.assertEqual(sst_key.get_metadata('ancestors'), '["lol"]')
    self.assertEqual(sst_key.get_metadata('siblings'), '["cat"]')
    self.assertFalse(list(records))

    # Test with overflow.
    records = generate_records(n=25000)
    sst_key, records = create_sstable(records, self.bucket,
                                      directory=DIRECTORY)
    self.assertTrue(sst_key.size >= SSTable.MIN_SIZE)
    self.assertTrue(sst_key.size <= SSTable.MAX_SIZE)
    self.assertTrue(self.bucket.get_key(sst_key.name.rstrip('.sst') + '.idx'))
    sst_key = self.bucket.get_key(sst_key.name)
    for key in SSTable.METADATA_KEYS:
      self.assertTrue(sst_key.get_metadata(key) is not None)
    records = list(records)
    self.assertTrue(len(records) > 0)
    self.assertEqual(int(sst_key.get_metadata('num_records')) + len(records),
                     25000)

  def test_iterator(self):
    # Test forward and backwards iteration.
    for reverse, iter_wrapper in ((False, lambda l: l),
                                  (True, lambda l: l[::-1])):
      records = list(generate_records(n=10000))
      sst_key, _ = create_sstable(iter(records), self.bucket,
                                  directory=DIRECTORY)
      self.assertEqual(len(list(_)), 0)
      sstable = SSTable(self.bucket, sst_key.name)

      # Fetch entire SSTable.
      records_from_sst = list(sstable.iterator(reverse=reverse))
      self.assertEqual(iter_wrapper(records), records_from_sst)
      # Fetch using random start_id, end_id pairs from records.
      for _ in xrange(20):
        start_idx = random.randint(0, len(records) - 1)
        try:
          end_idx = random.randint(start_idx, len(records) - 1)
        except ValueError:
          pass
        records_from_sst = list(sstable.iterator(start_id=records[start_idx].id,
                                                 end_id=records[end_idx].id,
                                                 reverse=reverse))
        self.assertEqual(iter_wrapper(records[start_idx:end_idx + 1]),
                         records_from_sst)

  def test_index(self):
    records = list(generate_records(n=1000))
    sst_key, _ = create_sstable(iter(records), self.bucket,
                                directory=DIRECTORY)
    self.assertEqual(len(list(_)), 0)
    sstable = SSTable(self.bucket, sst_key.name)
    index = sstable.index
    self.assertTrue(index.is_consistent())

    start, end = index.get_data_range(records[0].id, records[-1].id)
    self.assertEqual(start, 0)
    self.assertEqual(end, sstable.size)
    
    old_end = None
    for start, end in index.get_block_offsets(records[0].id, records[-1].id,
                                              False):
      if old_end is None:
        self.assertEqual(start, 0)
      else:
        self.assertEqual(start, old_end)
      old_end = end
    self.assertEqual(old_end, sstable.size)
    self.assertEqual(list(index.get_block_offsets(records[0].id, records[-1].id,
                                                  False)),
                     list(index.get_block_offsets(records[0].id, records[-1].id,
                                                  True))[::-1])
