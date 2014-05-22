import bisect
import cStringIO
import re
import types
import tempfile
import uuid
import zlib

from boto.s3.key import Key

from kronos.utils.math import bytearray_to_hex
from kronos.utils.math import hex_to_bytearray

nop_types = types.StringTypes + (bytesarray, )

def dumps(obj, compress=False):
  _type = type(obj)

  if isinstance(_type, nop_types):
    value = obj
  elif _type == dict:
  
  if compress:
    string = zlib.compress(string)
  
  return ''.join([chr(len(key)), key, chr(len(value)), value])


def generate_key(start_key, end_key, level, version, extension, key_prefix):
  start_key = bytearray_to_hex(start_key)
  end_key = bytearray_to_hex(end_key)
  key =  'L-%d:V-%d:%s:%s.%s' % (level, version, start_key, end_key,
                                 extension)
  key_prefix = key_prefix.replace(':', '')
  if key_prefix:
    key = '%s:%s' % (key_prefix, key)
  return key

class FileWrapper(object):
  def read(self, size=None):
    pass

class SSTableEntry(object):
  def __init__(self, key, value):
    self.key = key
    self.value = value


class SSTableSerializer(object):
  @staticmethod
  def read(self, n=-1):
    while True:
      key_size = ord(self.buffer.read(1))
      key = bytearray(self.read(key_size))
      val_size = ord(read(1))
      value = read(val_size)
      if decompress:
        value = zlib.decompress(value)
      yield key, value

  def write(self, entry):
    key_len = chr(len(entry.key))
    val = zlib.compress(entry.val) if self.compress else entry.val
    val_len = chr(len(val))
    self.stream.write(''.join([key_len, entry.key, key_len,
                               val_len, val, val_len]))


class SSTableError(Exception):
  pass


class SSTableIndex(object):
  def __init__(self, sstable):
    self.sstable = sstable
    self.key = '%sidx' % sstable.key[:-3] # Change extension.
    self.idx = {'keys': [], 'offsets': []}

    if not sstable.size:
      return

    data = sstable.bucket.get_key(self.key).get_contents_as_string()
    for line in data.split('\n'):
      if not line:
        break
      key, offset = loads(line)
      # Index is stored in sorted order, so no need to sort it again.
      self.idx['keys'].append(bytearray(key))
      self.idx['offsets'].append(int(offset))

  @staticmethod
  def generate_key(start_key, end_key, level, version, key_prefix=''):
    return generate_key(start_key, end_key, level, version, 'idx', key_prefix)
  
  def get_offsets(self, start_key, end_key):
    if start_key is None:
      min_bytes = 0
    else:
      i = bisect.bisect_left(self.idx['keys'], start_key)
      min_bytes = self.idx['offsets'][i]
    if end_key is None:
      max_bytes = self.sstable.size
    else:
      i = bisect.bisect_right(self.idx['keys'], end_key)
      if i == len(self.idx['keys']):
        max_bytes = self.sstable.size
      else:
        max_bytes = self.idx['offsets'][i]
    return (min_bytes, max_bytes)


class SSTable(object):
  KEY_REGEX = re.compile('((?P<prefix>.+):)?L-(?P<level>\d+):'
                         'V-(?P<version>\d+):(?P<start_key>.+):'
                         '(?P<end_key>.+)')
  # Index file size would be approximately (4096 / 4) * (4 + 16) bytes = 20kb.
  INDEX_BLOCK_SIZE = 1024 * 1024 * 4 # 4mb
  SIZE_THRESHOLD = 1024 * 1024 * 1024 * 4 # 4gb
  
  def __init__(self, bucket, start_key, end_key, level, version, key_prefix=''):
    self.level = level
    self.bucket = bucket
    self.start_key = start_key
    self.end_key = end_key
    
    self.key = SSTable.generate_key(start_key, end_key, level, version,
                                    key_prefix)

    self.size = getattr(bucket.get_key(self.key), 'size', 0)
    self.index = SSTableIndex(self)

  @staticmethod
  def generate_key(start_key, end_key, level, version, key_prefix=''):
    return generate_key(start_key, end_key, level, version, 'sst', key_prefix)
  
  @classmethod
  def from_key(cls, bucket, key):
    if key.endswith('.sst') or key.endswith('.idx'):
      key = key[:-4]
    match = cls.KEY_REGEX.match(key)
    if not match:
      raise KeyError
    return cls(bucket,
               hex_to_bytearray(match.group('start_key')),
               hex_to_bytearray(match.group('end_key')),
               int(match.group('level')),
               int(match.group('version')),
               key_prefix=match.group('prefix') or '')
  
  def iterator(self, start_key=None, end_key=None, reverse=False):
    min_byte, max_byte = self.index.get_offsets(start_key, end_key)

    response = self.bucket.connection.make_request(
      'GET',
      bucket=self.bucket.name,
      key=self.key,
      headers={'Range': 'bytes=%d-%d' % (min_byte, max_byte)}
      )

    size = 0
    with tempfile.TemporaryFile() as tmp_file:
      # Read SSTable into temporary file.
      while True:
        data = response.read(1024)
        size += len(data)
        if not data:
          break
        tmp_file.write(data)

      # Ensure that the expected size of the data was downloaded.
      assert size == max_byte - min_byte

      tmp_file.seek(0, 2 if reverse else 0)
      event_buffer = []

      def get_kv():
        global event_buffer
        event_str = ''.join(event_buffer)
        event_buffer = []
        return loads(event_str.rstrip('\n'), decompress=True)
      
      while size:
        read_size = min(8192, size)
        size -= read_size
        
        if reverse:
          tmp_file.seek(-read_size, 2)
          data = tmp_file.read(read_size)
          # Seek to the point till where we have read.
          tmp_file.seek(-read_size, 2)
          if '\n' in data:
            data, last_chunk = data.split('\n')
            event_buffer.insert(0, last_chunk)
            yield get_kv()
          event_buffer.insert(0, data)
        else:
          data = tmp_file.read(read_size)
          if '\n' in data:
            last_chunk, data = data.split('\n')
            event_buffer.append(last_chunk)
            yield get_kv()
          event_buffer.append(data)

def create_sstable(bucket, level, version, data, key_prefix=''):
  index = []
  size = 0
  index_block = 0
  start_key = None
  end_key = None

  tmp_key = uuid.uuid4()

  # Create temporary data file and upload it to S3.
  with tempfile.TemporaryFile() as tmp_file:
    for key, value in data:
      if size >= SSTable.SIZE_THRESHOLD:
        break
      if start_key is None:
        start_key = key
      index_block = size / SSTable.INDEX_BLOCK_SIZE
      if index_block == len(index):
        index.append((key, size))
      line = dumps(key, value, compress=True)
      tmp_file.write(line)
      size += len(line)
      end_key = key

    Key(bucket, '%s.sst' % tmp_key).set_contents_from_file(tmp_file,
                                                           rewind=True)
      
  # Upload index file.
  index_str = cStringIO.StringIO()
  map(lambda d: index_str.write(dumps(*d)), index)
  Key(bucket, '%s.idx' % tmp_key).set_contents_from_string(index_str.getvalue())
  index_str.close()

  # Copy over to canonical keys.
  sst_key = SSTable.generate_key(start_key, end_key, level, version, key_prefix)
  idx_key = SSTableIndex.generate_key(start_key, end_key, level, version,
                                      key_prefix)
  bucket.copy_key(sst_key, bucket.name, '%s.sst' % tmp_key)
  bucket.copy_key(idx_key, bucket.name, '%s.idx' % tmp_key)
  bucket.delete_key('%s.sst' % tmp_key)
  bucket.delete_key('%s.idx' % tmp_key)

  return SSTable.from_key(bucket, sst_key)
