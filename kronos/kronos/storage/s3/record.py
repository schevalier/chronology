import cPickle
import sys
import types

from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.utils.math import uuid_to_kronos_time
from kronos.utils.uuid import TimeUUID


# Guesstimate compression factor from record.size => compress(record.marshall).
# TODO(usmanm): Ideally we should have some streaming compression algorithm, for
# accurate consistent sizing of *index blocks*.
COMPRESS_FACTOR = 0.6


class RecordType(object):
  DELETE = 0
  EVENT = 1
  INDEX = 2


class Record(object):
  # sizeof(self) + sizeof(self.type) + sizeof(self.time) + sizeof(self.id) +
  # sizeof(self._cmp_value)
  BASE_SIZE = 272

  def __init__(self, _type, _id):
    self.type = _type
    self.id = _id
    uuid = TimeUUID(_id)
    self.time = uuid_to_kronos_time(uuid)
    # Same comparison that `Log` uses.
    self._cmp_value = uuid.to_lexicographic_str()

  def marshall(self):
    return cPickle.dumps(self, cPickle.HIGHEST_PROTOCOL)

  def __getitem__(self, name):
    if name == ID_FIELD:
      return self.id
    elif name == TIMESTAMP_FIELD:
      return self.time
    raise KeyError

  def __cmp__(self, other):
    if other is None:
      return 1
    if isinstance(other, types.StringTypes):
      cmp_value = TimeUUID(other).to_lexicographic_str()
    elif isinstance(other, Record):
      cmp_value = other._cmp_value
    elif isinstance(other, TimeUUID):
      cmp_value = other.to_lexicographic_str()
    else:
      raise TypeError
    return cmp(self._cmp_value, cmp_value)

  @staticmethod
  def unmarshall(s):
    return cPickle.loads(s)


class DeleteRecord(Record):
  def __init__(self, start_id, end_id):
    super(DeleteRecord, self).__init__(RecordType.DELETE, start_id)
    self.end_id = end_id

  @property
  def start_id(self):
    return self.id

  @property
  def size(self):
    # sizeof(self.start_id) + sizeof(self.end_id)
    return 158 + Record.BASE_SIZE

class EventRecord(Record):
  def __init__(self, _dict):
    super(EventRecord, self).__init__(RecordType.EVENT, _dict[ID_FIELD])
    self.dict = _dict

  def __getitem__(self, name):
    return self.dict.get(name, super(EventRecord, self).__getitem__(name))

  @property
  def size(self):
    return sys.getsizeof(self.dict) + Record.BASE_SIZE


class IndexRecord(Record):
  def __init__(self, start_id, offset, has_delete):
    super(IndexRecord, self).__init__(RecordType.INDEX, start_id)
    self.offset = offset
    self.has_delete = has_delete

  @property
  def size(self):
    # sizeof(self.offset) + sizeof(self.has_delete) + sizeof(self.start_id)
    return 133 + Record.BASE_SIZE
