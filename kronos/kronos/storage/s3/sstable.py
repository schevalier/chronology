import bisect
import cStringIO
import tempfile
import types
import uuid

from boto.s3.key import Key


# Index file size would be approximately (2048 / 2) * (2 + 4 + 16) bytes = 22kb.
INDEX_BLOCK_SIZE = 1024 * 1024 * 4 # 2mb
MIN_SIZE = 1024 * 1024 * 1024 # 1gb
MAX_SIZE = 1024 * 1024 * 1024 * 2 # 2gb
LENGTH_BITMASK = 0x7fff # 31 LSBs.
DELETION_MARKER_BITMASK = 0xffff ^ LENGTH_BITMASK


def _serialize(value, include_footer=True, is_deletion_marker=False):
  value = str(value)
  if len(value) > LENGTH_BITMASK:
    raise ValueError
  header = len(value) | (DELETION_MARKER_BITMASK if is_deletion_marker else 0)
  return '%d%d%s' % (header, len(value), header if include_footer else '')

class Reader(object):
  class Type:
    STRING = 0
    FILE = 1
  
  def __init__(self, obj, _type, reverse):
    self.reverse = reverse
    self.obj = obj
    self.type = _type
    self.cursor = 0

  @classmethod
  def from_string(cls, s, reverse=False):
    return cls(s, Reader.Type.STRING, reverse)

  @classmethod
  def from_file(cls, f, reverse=False):
    return cls(f, Reader.Type.FILE, reverse)

  def reset(self):
    if self.type == Reader.Type.FILE:
      self.obj.seek(0)
    self.cursor = 0

  def read(self, n):
    if self._type == Reader.Type.STRING:
      if self.reverse:
        pass
      else:
        yield self.obj[self.cursor: self.cursor + n]
    elif self._type == Reader.Type.FILE:
      pass
    self.cursor += (-1 if self.reverse else 1) * n


class SSTableError(Exception):
  pass


class SSTableIndex(object):
  def __init__(self, sstable):
    self.sstable = sstable
    self.key = sstable.key.rstrip('.sst') + '.idx'
    self.idx = {'keys': [],
                'offsets': []}

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
