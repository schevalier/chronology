import itertools
import leveldb
import os
import time
import types
import uuid

from kronos.storage.s3.record import DeleteRecord
from kronos.storage.s3.record import Record
from kronos.storage.s3.sstable import create_sstable
from kronos.utils.uuid import TimeUUID
from kronos.utils.math import uuid_from_time


SIZE_OF_ID_BYTES = 26 + 16 # 26 for str(datetime) + 16 for uuid.bytes
START_KEY = ''
END_KEY = '\xff'


def get_bytes_from_id(_id):
  if isinstance(_id, types.StringTypes):
    try:
      _id = TimeUUID(_id)
    except ValueError:
      # Not UUID type.
      pass
  if isinstance(_id, TimeUUID):
    return _id.to_lexicographic_str()
  if isinstance(_id, uuid.UUID):
    return TimeUUID(_id).to_lexicographic_str()
  return str(_id)


def _generate_key(stream, id_or_bytes):
  return '%s%s' % (stream, get_bytes_from_id(id_or_bytes))


class LevelDBStore(object):  
  def __init__(self, dir_path, _id=None):
    self._id = _id or str(uuid_from_time(time.time()))
    if not os.path.exists(dir_path):
      os.makedirs(dir_path)
    self._path = '%s/%s' % (dir_path.rstrip('/'), self._id)
    self._db = leveldb.LevelDB(self._path)

  def destroy(self):
    del self._db
    leveldb.DestroyDB(self._path)

  def insert(self, stream, record):
    self._db.Put(_generate_key(stream, record.id), record.marshall())

  def get(self, stream, _id):
    record = self._db.Get(_generate_key(stream, _id))
    if record:
      return Record.unmarshall(record)

  def delete(self, stream, start_id=START_KEY, end_id=END_KEY):
    for key in self._db.RangeIter(_generate_key(stream, start_id),
                                  _generate_key(stream, end_id),
                                  include_value=False):
      self._db.Delete(key)
    self.insert(stream, DeleteRecord(start_id, end_id))
    
  def stream_iterator(self, stream, start_id=START_KEY, end_id=END_KEY):
    return (Record.unmarshall(value) for key, value in
            self._db.RangeIter(_generate_key(stream, start_id),
                               _generate_key(stream, end_id)))

  def stream_iterators(self):
    """ Returns an iterator of (stream, iterator) tuples, where the iterator in
    the tuple will yield all events for that partcular stream. Streams are
    yielded in lexicographic order.

    NOTE: If you call next() on this function the previous (if any) stream
    iterator is no longer valid.
    """
    current_stream = None
    # Wrap in a list. Issue caused by non availability of the `nonlocal`
    # keyword in Python 2.7
    global_iterator = [
      ((key[:-SIZE_OF_ID_BYTES], Record.unmarshall(value))
       for key, value in self._db.RangeIter(START_KEY, END_KEY))
      ]

    def stream_iterator(current_stream):
      for stream, event in global_iterator[0]:
        if stream != current_stream:
          global_iterator[0] = itertools.chain([(stream, event)],
                                               global_iterator[0])
          raise StopIteration
        yield event

    while True:
      # Poll to see what the stream name is.
      stream, event = global_iterator[0].next()
      while current_stream and stream == current_stream:
        stream, event = global_iterator[0].next()
      current_stream = stream
      global_iterator[0] = itertools.chain([(stream, event)],
                                           global_iterator[0])
      yield (stream, stream_iterator(current_stream))

  def size(self):
    """ Returns size of log in bytes. """
    total_size = 0
    for path, _, files in os.walk(self.path):
      for name in files:
        total_size += os.path.getsize(os.path.join(path, name))
    return total_size


class MemTable(object):
  def __init__(self, bucket, dir_path):
    self.bucket = bucket
    self.dir_path = dir_path

    self.flush()
    self.recover()

  def flush(self):
    old_store = getattr(self, 'current_store', None)
    self.current_store = LevelDBStore(self.dir_path)
    if old_store:
      self.async_push_store_to_s3(old_store)


  def push_store_to_s3(self, store):
    sst_keys = []
    for stream, records in store.stream_iterators():
      sst_key, _ = create_sstable(self.bucket, stream, records, level=0,
                                  memtable_id=store._id, split=False)
      sst_keys.append(sst_key)
    
    store.destroy()

  def async_push_store_to_s3(self):
    self.push_store_to_s3()

  def recover(self):
    # TODO(usmanm): Do this.
    pass

  def async_recover(self):
    pass
