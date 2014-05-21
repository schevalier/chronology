import leveldb
import os
import time

from kronos.storage.backends.s3.constants import MAX_KEY
from kronos.storage.backends.s3.constants import MIN_KEY

# Analagous to MemTable in LevelDB but really an on-disk LevelDB store rather
# than an in-memory store.
class MemTable(object):  
  def __init__(self, dir_path, db_name=None):
    db_name = db_name or str(int(time.time()))
    self.path = '%s/%s' % (dir_path.rstrip('/'), db_name)
    self.db = leveldb.LevelDB(self.path)

  @staticmethod
  def destory(path):
    return leveldb.DestroyDB(path)

  def insert(self, key, value):
    self.db.Put(key, value)

  def scan(self, start_key=MIN_KEY, end_key=MAX_KEY):
    return self.db.RangeIter(start_key, end_key)

  def size(self):
    total_size = 0
    for path, _, files in os.walk(self.path):
      for name in files:
        total_size += os.path.getsize(os.path.join(path, name))
    return total_size
