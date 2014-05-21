import bisect
import boto
import cStringIO
import itertools
import tempfile


def read_kv(line):
  line = line.rstrip('\n')
  key_size = int(line[0])
  return bytearray(line[1:1+key_size]), line[1+key_size:]


def write_kv(buf, key, value):
  buf.write('%d%s%s\n' % (len(key), key, value))


class SSTableError(Exception):
  pass


class SSTableIndex(object):
  def __init__(self, sstable):
    self.sstable = sstable
    self.key = '%s.idx' % sstable.key[:-4]

    self.idx = {'keys': [], 'offsets': []}
    if not sstable.size:
      return

    data = (boto.s3.Bucket(sstable.s3_connection, sstable.bucket)
            .get_key(self.key)
            .get_contents_as_string())
    for line in data.split('\n'):
      key, offset = line.split(':')
      # Index is stored in sorted order, so no need to sort it again.
      self.idx['keys'].append(bytearray(key))
      self.idx['offset'].append(int(offset))
      
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
  SIZE_THRESHOLD = 1024 * 1024 * 1024 * 4 # 4gb
  
  def __init__(self, s3_connection, bucket, level, start_key, end_key,
               version, key_prefix=''):
    self.connection = s3_connection
    self.level = level
    self.bucket = bucket
    self.start_key = start_key
    self.end_key = end_key
    
    self.key = 'l-%d:v-%d:%s:%s.sst' % (level, version, start_key, end_key)
    key_prefix = key_prefix.replace(':', '')
    if key_prefix:
      self.key = '%s:%s' % (key_prefix, self.key)

    s3_key = boto.s3.Bucket(s3_connection, bucket).get_key(self.key)
    if s3_key.exists():
      self.size = int(s3_key.get_metadata('Content-Length'))
    else:
      self.size = None

    self.index = SSTableIndex(self)
  
  def iterator(self, start_key=None, end_key=None, reverse=False):
    min_byte, max_byte = self.index.get_offsets(start_key, end_key)

    response = self.connection.make_request(
      'GET',
      bucket=self.bucket,
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
        return read_kv(event_str)
      
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

  def write(self, data):
    if self.size:
      raise SSTableError('SSTable files are immutable.')

    index = []
    size = 0

    for key, value in data:
      if size >= SSTable.SIZE_THRESHOLD:
        break
    
    # Upload index file.
    index_str = cStringIO.StringIO()
    map(lambda key, value: write_kv(index_str, key, value), index)
    #self.index.set_contents_from_file(index_str.getvalue())
    index_str.close()

    # Rename index file

    # Reset size and index.
    self.size = size
    self.index = SSTableIndex(self)
