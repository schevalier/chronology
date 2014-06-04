import json
import unittest

from collections import defaultdict

from kronos.conf import settings
from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.storage.s3.log import Log
from kronos.storage.s3.record import RecordType
from tests.storage.s3 import generate_records


class TestLog(unittest.TestCase):
  def setUp(self):
    self.log = Log(settings.storage.s3.local_dir)

  def tearDown(self):
    self.log.destroy()

  def assert_sorted(self, records):
    t = float('-inf')
    for record in records:
      self.assertTrue(record[TIMESTAMP_FIELD] >= t)
      t = record[TIMESTAMP_FIELD]

  def test_stream_iterator(self):
    records = list(generate_records(n=100))
    for record in records:
      self.log.insert('stream', record)

    # Try reading all records.
    records_from_log = list(self.log.stream_iterator('stream'))
    self.assertEqual(len(records_from_log), len(records))
    self.assertEqual(
      set(record[TIMESTAMP_FIELD] for record in records_from_log),
      set(record[TIMESTAMP_FIELD] for record in records))
    self.assert_sorted(records_from_log)

    # Try reading some smaller ID slice.
    records_from_log = list(self.log.stream_iterator('stream',
                                                     records[30][ID_FIELD],
                                                     records[69][ID_FIELD]))
    self.assertEqual(len(records_from_log), len(records[30:70]))
    self.assertEqual(
      set(record[TIMESTAMP_FIELD] for record in records_from_log),
      set(record[TIMESTAMP_FIELD] for record in records[30:70]))
    self.assert_sorted(records_from_log)

  def test_iterator(self):
    records = list(generate_records(n=100))
    streams = {'lol', 'cat'}
    for stream in streams:
      for record in records:
        self.log.insert(stream, record)
    
    records_from_log = list(self.log.iterator())
    self.assertEqual(len(records_from_log), 200)
    stream_records = defaultdict(list)
    last_stream = ''
    for stream, record in records_from_log:
      self.assertTrue(last_stream <= stream)
      stream_records[stream].append(record)
      last_stream = stream
    self.assertEqual(len(stream_records), 2)
    self.assertEqual(set(stream_records), streams)
    for records in stream_records.itervalues():
      self.assertEqual(len(records), 100)
      self.assert_sorted(records)
    stream_records = [map(lambda e: json.dumps(e.dict, sort_keys=True), records)
                      for records in stream_records.itervalues()]
    self.assertEqual(stream_records[0], stream_records[1])

  def test_delete(self):
    records = list(generate_records(n=100))
    for record in records:
      self.log.insert('stream', record)
    
    # Try reading all records.
    records_from_log = list(self.log.stream_iterator('stream'))
    self.assertEqual(len(records_from_log), len(records))
    self.assertEqual(
      set(record[TIMESTAMP_FIELD] for record in records_from_log),
      set(record[TIMESTAMP_FIELD] for record in records))
    self.assert_sorted(records_from_log)

    # Delete some range (total of 41 deleted)
    self.log.delete('stream', records[30][ID_FIELD], records[70][ID_FIELD])

    # Try reading all records.
    records_from_log = list(self.log.stream_iterator('stream'))
    self.assertEqual(len(records_from_log), 60) # 59 records + 1 delete record.
    self.assertEqual(
      set(record[TIMESTAMP_FIELD] for record in records_from_log),
      # Include record[30] because delete record will be with that
      # timestamp.
      set(record[TIMESTAMP_FIELD] for record in records[:31] + records[71:]))
    self.assertTrue(len(map(lambda r: r.type == RecordType.DELETE,
                            records_from_log)), 59)
    self.assert_sorted(records_from_log)

    # Ensure delete record present.
    delete_record = self.log.get('stream', records[30][ID_FIELD])
    self.assertEqual(delete_record.type, RecordType.DELETE)
    self.assertEqual(delete_record[ID_FIELD], records[30][ID_FIELD])
    self.assertEqual(delete_record.end_id, records[70][ID_FIELD])
