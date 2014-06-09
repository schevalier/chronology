import bisect
import cPickle
import itertools
import json
import os
import tempfile
import types
import zlib

from boto.s3.key import Key

from kronos.storage.s3.errors import SSTableError
from kronos.storage.s3.errors import SSTableMalformed
from kronos.storage.s3.errors import SSTableMissing
from kronos.storage.s3.record import COMPRESS_FACTOR
from kronos.storage.s3.record import IndexRecord
from kronos.storage.s3.record import RecordType
from kronos.utils.uuid import HIGHEST_UUID
from kronos.utils.uuid import LOWEST_UUID
from kronos.utils.uuid import TimeUUID


SST_KEY = 'sstables/{stream}/sst_{start_id}'
IDX_KEY = 'sstables/{stream}/idx_{start_id}'


def marshall(records):
  return zlib.compress(cPickle.dumps(records, cPickle.HIGHEST_PROTOCOL), 9)

def unmarshall(dump):
  return cPickle.loads(zlib.decompress(dump))


class SSTableIndex(object):
  def __init__(self, sstable):
    self.sstable = sstable
    self.key = sstable.bucket.get_key(sstable.key.name.replace('sst_', 'idx_'))
    if self.key is None:
      raise SSTableMalformed('idx file missing.')

    self.records = unmarshall(self.key.get_contents_as_string())
    assert self.is_consistent()

  def is_consistent(self):
    return (all(record.type == RecordType.INDEX for record in self.records) and
            self.records == sorted(self.records))

  def _get_start_idx(self, _id):
    _id = IndexRecord(_id, None, None)
    i = bisect.bisect_left(self.records, _id)
    assert i >= 0
    if i != 0 and self.records[i] != _id:
      i -= 1
    return i

  def _get_end_idx(self, _id):
    _id = IndexRecord(_id, None, None)
    i = bisect.bisect_right(self.records, _id)
    assert i <= len(self.records)
    if i != len(self.records) and self.records[i] == _id:
      i += 1
    return i
  
  def data_offsets(self, start_id, end_id):
    start_bytes = self.records[self._get_start_idx(start_id)].offset
    end_idx = self._get_end_idx(end_id)
    if end_idx == len(self.records):
      end_bytes = self.sstable.size
    else:
      end_bytes = self.records[end_idx].offset
    return (start_bytes, end_bytes)

  def block_offsets(self, start_id, end_id, reverse):
    """ Yields a sequence of (start_bytes, end_bytes) tuples for all
    compressed blocks contained events for [start_id, end_id] """
    start_idx = self._get_start_idx(start_id)
    end_idx = self._get_end_idx(end_id)
    if reverse:
      it = xrange(end_idx - 1, start_idx - 1, -1)
    else:
      it = xrange(start_idx, end_idx)
    for i in it:
      if i == len(self.records) - 1:
        end_bytes = self.sstable.size
      else:
        end_bytes = self.records[i + 1].offset
      yield (self.records[i].offset, end_bytes)

  def has_delete(self, start_id, end_id):
    start_idx = self._get_start_idx(start_id)
    end_idx = self._get_end_idx(end_id)
    return any(self.records[i].has_delete for i in xrange(start_idx, end_idx))


class SSTable(object):
  INDEX_BLOCK_SIZE = 1024 * 1024 * 2 # 2mb
  MIN_SIZE = 1024 * 1024 * 1024 * 1 # 1gb
  MAX_SIZE = 1024 * 1024 * 1024 * 2 # 2gb
  VERSION = 1
  METADATA_KEYS = ('start_id',
                   'end_id',
                   'has_delete',
                   'ancestors',
                   'siblings',
                   'size',
                   'version',
                   'level',
                   'memtable_id',
                   'num_records')
  
  def __init__(self, bucket, key):
    self.bucket = bucket
    if isinstance(key, types.StringTypes):
      self.key = bucket.get_key(key)
    else:
      self.key = key

    if self.key is None:
      raise SSTableMissing('%s:%s does not exist.' % (bucket.name, key))
    
    self.size = self.key.size
    for key in SSTable.METADATA_KEYS:
      metadata = self.key.get_metadata(key)
      if metadata is None:
        raise SSTableMalformed('`%s` metadata is missing.' % key)
      setattr(self, key, json.loads(metadata))
    self.start_id = TimeUUID(self.start_id)
    self.end_id = TimeUUID(self.end_id)

  @property
  def index(self):
    if not hasattr(self, '_index'):
      self._index = SSTableIndex(self)
    return self._index

  def contains_delete(self, start_id=LOWEST_UUID, end_id=HIGHEST_UUID):
    return self.index.has_delete(start_id, end_id)
    
  def iterator(self, start_id=LOWEST_UUID, end_id=HIGHEST_UUID, reverse=False):
    min_byte, max_byte = self.index.data_offsets(start_id, end_id)

    response = self.bucket.connection.make_request(
      'GET',
      bucket=self.bucket.name,
      key=self.key,
      headers={
        'Range': 'bytes=%d-%d' % (
          min_byte,
          # S3 returns data inclusive of max_byte
          max_byte if max_byte == self.size else max_byte - 1
          )
        }
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

      tmp_file.seek(0)
      records = []

      for start_offset, end_offset in self.index.block_offsets(start_id,
                                                               end_id,
                                                               reverse):
        if min_byte:
          start_offset -= min_byte
          end_offset -= min_byte
        if tmp_file.tell() != start_offset:
          tmp_file.seek(start_offset)
        block_data = tmp_file.read(end_offset - start_offset)
        records.extend(unmarshall(block_data))
        if reverse:
          records.reverse()
        while records:
          record = records.pop(0)
          if record < start_id:
            if reverse:
              break
            else:
              continue
          elif record > end_id:
            if reverse:
              continue
            else:
              break
          yield record
        records[:] = []


def create_sstable(bucket, stream, records, level=0, version=SSTable.VERSION,
                   siblings=None, ancestors=None, memtable_id=None, split=True):
  class State(object):
    def __init__(self):
      self.reset()
      
    def reset(self):
      self.has_delete = False
      self.size = 0
      self.start_id = None
      self.end_id = LOWEST_UUID
      self.records = []

  siblings = siblings or []
  ancestors = ancestors or []
  remaining_records = records
  num_records = 0
  block_state = State()
  sst_state = State()
  max_delete = LOWEST_UUID
  tmp_file = tempfile.NamedTemporaryFile(delete=False)

  def flush_index_block():
    if sst_state.start_id is None:
      sst_state.start_id = block_state.start_id
    sst_state.end_id = max(sst_state.end_id, block_state.end_id)
    sst_state.has_delete |= block_state.has_delete
    string = marshall(block_state.records)
    tmp_file.write(string)
    sst_state.records.append(IndexRecord(block_state.start_id,
                                         sst_state.size, # offset
                                         block_state.has_delete))
    sst_state.size += len(string)
    block_state.reset()

  for record in records:
    end_id = record.end_id if record.type == RecordType.DELETE else record.id
    block_state.end_id = max(block_state.end_id, TimeUUID(end_id))

    # Should we start a new index block?
    if block_state.size > SSTable.INDEX_BLOCK_SIZE:
      flush_index_block()

    if split and sst_state.size >= SSTable.MIN_SIZE:
      # The current record must be consumed again, since it wasn't
      # added to the record list.
      remaining_records = itertools.chain((record, ), remaining_records)
      break

    if block_state.start_id is None:
      block_state.start_id = TimeUUID(record.id)
    block_state.size += (record.size * COMPRESS_FACTOR)
    if record.type == RecordType.DELETE:
      max_delete = max(max_delete, block_state.end_id)
      block_state.has_delete = True
    elif block_state.end_id > max_delete:
      max_delete = LOWEST_UUID # Reset delete marker.
    else:
      block_state.has_delete |= (block_state.start_id <= max_delete and
                                 block_state.end_id <= max_delete)
    block_state.records.append(record)
    num_records += 1
  else:
    # `records` are exhausted, flush the last index block state.
    if block_state.records:
      flush_index_block()

  tmp_file.close()

  # Upload index file.
  index_key = Key(bucket, IDX_KEY.format(stream=stream,
                                         start_id=str(sst_state.start_id)))
  index_data = marshall(sst_state.records)
  num_bytes = index_key.set_contents_from_string(index_data)
  assert num_bytes == len(index_data)

  # Upload data file.
  sst_key = Key(bucket, SST_KEY.format(stream=stream,
                                       start_id=str(sst_state.start_id)))
  if sst_key.exists():
    raise SSTableError('sst file already exists.')
  for key in SSTable.METADATA_KEYS:
    if hasattr(sst_state, key):
      value = getattr(sst_state, key)
    elif key in create_sstable.func_code.co_varnames:
      value = locals()[key]
    else:
      raise KeyError(key)
    try:
      value = json.dumps(value)
    except TypeError:
      value = json.dumps(str(value))
    sst_key.set_metadata(key, value)
  num_bytes = sst_key.set_contents_from_filename(tmp_file.name)
  assert num_bytes == sst_state.size

  # Clean up temp file.
  os.unlink(tmp_file.name)

  return (sst_key, remaining_records)
