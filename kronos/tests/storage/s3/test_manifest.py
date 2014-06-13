from kronos.storage.s3.manifest import Manifest
from kronos.storage.s3.sstable import create_sstable
from kronos.utils.math import uuid_from_time
from kronos.utils.math import uuid_to_time
from tests.storage.s3 import generate_records
from tests.storage.s3 import S3TestCase


class TestManifest(S3TestCase):
  def test_data_structures(self):
    ranges = set([(100, 1100), (200, 1200), (500, 1500), (1400, 2400)])
    create_sstable(self.bucket, 'test_overlapping_ssts',
                   generate_records(start_time=100, n=101)) # [100, 1100]
    create_sstable(self.bucket, 'test_overlapping_ssts',
                   generate_records(start_time=200, n=101)) # [200, 1200]
    create_sstable(self.bucket, 'test_overlapping_ssts',
                   generate_records(start_time=500, n=101)) # [500, 1500]
    create_sstable(self.bucket, 'test_overlapping_ssts',
                   generate_records(start_time=1400, n=101)) # [1400, 2400]

    manifest = Manifest(self.bucket)
    self.assertEqual(
      set(map(lambda interval:
              tuple(map(lambda uuid: int(uuid_to_time(uuid)),
                        interval)),
              manifest.sstables['test_overlapping_ssts'])),
      ranges)
    self.assertEqual(
      set(map(lambda interval:
              tuple(map(lambda uuid: int(uuid_to_time(uuid)),
                        interval)),
              manifest.sstables['test_overlapping_ssts'])),
      ranges)

  def test_overlapping_ssts_and_stream_isolation(self):
    streams = ('test_overlapping_ssts1', 'test_overlapping_ssts2')
    for stream in streams:
      create_sstable(self.bucket, stream,
                     generate_records(start_time=100, n=101)) # [100, 1100]
      create_sstable(self.bucket, stream,
                     generate_records(start_time=200, n=101)) # [200, 1200]
      create_sstable(self.bucket, stream,
                     generate_records(start_time=500, n=101)) # [500, 1500]
      create_sstable(self.bucket, stream,
                     generate_records(start_time=1400, n=101)) # [1400, 2400]

    manifest = Manifest(self.bucket)

    for stream in streams:
      tests = [(150, 300, 2), (400, 600, 3), (100, 1500, 4), (2000, 2100, 1)]
      for start_time, end_time, expected_ssts in tests:
        start_id = uuid_from_time(start_time)
        end_id = uuid_from_time(end_time)
        ssts = list(manifest.overlapping_ssts(stream,
                                              start_id, end_id))
        self.assertEqual(len(ssts), expected_ssts)
        for sst in ssts:
          self.assertTrue(stream in sst.key.name)
          self.assertTrue(sst.start_id <= end_id)
          self.assertTrue(sst.end_id >= start_id)
