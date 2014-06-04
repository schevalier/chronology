import leveldb
import os
import time
import types
import uuid

from kronos.storage.s3.record import DeleteRecord
from kronos.storage.s3.record import Record
from kronos.utils.uuid import TimeUUID


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


class Log(object):  
  def __init__(self, dir_path, db_name=None):
    db_name = db_name or str(int(time.time()))
    if not os.path.exists(dir_path):
      os.makedirs(dir_path)
    self.path = '%s/%s' % (dir_path.rstrip('/'), db_name)
    self._db = leveldb.LevelDB(self.path)

  def destroy(self):
    del self._db
    leveldb.DestroyDB(self.path)

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

  def iterator(self):
    """ Iterates over all events stored in the log. Events are yielded in
    lexicographical order of the stream, and within each stream events are
    yielded in ID sorted order. """
    return ((key[:-SIZE_OF_ID_BYTES], Record.unmarshall(value))
            for key, value in self._db.RangeIter(START_KEY, END_KEY))

  def size(self):
    """ Returns size of log in bytes. """
    total_size = 0
    for path, _, files in os.walk(self.path):
      for name in files:
        total_size += os.path.getsize(os.path.join(path, name))
    return total_size
