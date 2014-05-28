import json
import unittest

from collections import defaultdict

from kronos.conf import settings
from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.storage.s3.log import Log
from kronos.utils.math import time_to_kronos_time
from kronos.utils.math import uuid_from_time


def generate_events(start_time=10, interval=10, n=-1):
  t = start_time
  while n != 0:
    yield {TIMESTAMP_FIELD: time_to_kronos_time(t),
           ID_FIELD: str(uuid_from_time(t))}
    n -= 1
    t += interval


class TestLog(unittest.TestCase):
  def setUp(self):
    self.log = Log(settings.storage.s3.local_dir)

  def tearDown(self):
    self.log.destroy()

  def assert_sorted(self, events):
    t = float('-inf')
    for event in events:
      self.assertTrue(event[TIMESTAMP_FIELD] >= t)
      t = event[TIMESTAMP_FIELD]

  def test_stream_iterator(self):
    events = list(generate_events(n=100))
    for event in events:
      self.log.insert('stream', event)

    # Try reading all events.
    events_from_log = list(self.log.stream_iterator('stream'))
    self.assertEqual(len(events_from_log), len(events))
    self.assertEqual(set(event[TIMESTAMP_FIELD] for event in events_from_log),
                     set(event[TIMESTAMP_FIELD] for event in events))
    self.assert_sorted(events_from_log)

    # Try reading some smaller ID slice.
    events_from_log = list(self.log.stream_iterator('stream',
                                                    events[30][ID_FIELD],
                                                    events[69][ID_FIELD]))
    self.assertEqual(len(events_from_log), len(events[30:70]))
    self.assertEqual(set(event[TIMESTAMP_FIELD] for event in events_from_log),
                     set(event[TIMESTAMP_FIELD] for event in events[30:70]))
    self.assert_sorted(events_from_log)

  def test_iterator(self):
    events = list(generate_events(n=100))
    streams = {'lol', 'cat'}
    for stream in streams:
      for event in events:
        self.log.insert(stream, event)

    events_from_log = list(self.log.iterator())
    self.assertEqual(len(events_from_log), 200)
    stream_events = defaultdict(list)
    last_stream = ''
    for stream, event in events_from_log:
      self.assertTrue(last_stream <= stream)
      stream_events[stream].append(event)
      last_stream = stream
    self.assertEqual(len(stream_events), 2)
    self.assertEqual(set(stream_events), streams)
    for events in stream_events.itervalues():
      self.assertEqual(len(events), 100)
      self.assert_sorted(events)
    stream_events = [map(lambda e: json.dumps(e, sort_keys=True), events)
                     for events in stream_events.itervalues()]
    self.assertEqual(stream_events[0], stream_events[1])
