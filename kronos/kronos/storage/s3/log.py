import json
import leveldb
import os
import time
import uuid

from kronos.conf.constants import ID_FIELD

KEY_SIZE = 16 # Size of UUID: 128 bits (16 bytes).
START_KEY = ''
END_KEY = '\xff'


def _generate_key(stream, id):
  if isinstance(id, uuid.UUID):
    id = id.bytes
  else:
    try:
      id = uuid.UUID(id).bytes
    except ValueError:
      # Already in *bytes* format.
      pass
  return '%s%s' % (stream, id)


class Log(object):  
  def __init__(self, dir_path, db_name=None):
    db_name = db_name or str(int(time.time()))
    self.path = '%s/%s' % (dir_path.rstrip('/'), db_name)
    self.db = leveldb.LevelDB(self.path)

  @staticmethod
  def destory(path):
    return leveldb.DestroyDB(path)

  def insert(self, stream, event):
    self.db.Put(_generate_key(stream, event[ID_FIELD], json.dumps(event)))

  def stream_iterator(self, stream, start_key='', end_key='\xff'):
    return (json.loads(value) for key, value in
            self.db.RangeIter(_generate_key(stream, start_key),
                              _generate_key(stream, end_key)))

  def iterator(self, start='', end_key='\xff'):
    """ Iterates over all events stored in the log. Events are yielded in
    lexicographical order of the stream, and within each stream events are
    yielded in ID sorted order. """
    return ((key[:-KEY_SIZE], json.loads(value))
            for key, value in self.db.RangeIter('', '\xff'))

  def size(self):
    """ Returns size of log in bytes. """
    total_size = 0
    for path, _, files in os.walk(self.path):
      for name in files:
        total_size += os.path.getsize(os.path.join(path, name))
    return total_size
