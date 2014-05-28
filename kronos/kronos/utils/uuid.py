from __future__ import absolute_import

from datetime import datetime
from types import StringTypes
from uuid import UUID

from kronos.conf.constants import ResultOrder
from kronos.core.exceptions import InvalidTimeUUIDComparison


class UUIDType(object):
  LOWEST = 'lowest'
  HIGHEST = 'highest'
  RANDOM = 'random'


class TimeUUID(UUID):
  """
  Override Python's UUID comparator so that time is the first parameter used
  for sorting. This is the comparator behavior for `timeuuid` types in
  Cassandra.
  """
  def __init__(self, *args, **kwargs):
    """
    `order`[kwarg]: Whether to return the results in
             ResultOrder.ASCENDING or ResultOrder.DESCENDING
             time-order.
    """
    from kronos.utils.math import uuid_to_kronos_time

    # TODO(marcua): Couldn't get `order` to be a named arg (because of
    # subclassing?).  I don't like the next line.
    if args and isinstance(args[0], UUID):
      args = list(args)
      args[0] = str(args[0])
    order = kwargs.pop('order', ResultOrder.ASCENDING)
    kwargs['version'] = 1
    super(TimeUUID, self).__init__(*args, **kwargs)

    self._kronos_time = uuid_to_kronos_time(self)
    self._cmp_multiplier = ResultOrder.get_multiplier(order)
    
  def __setattr__(self, name, value):
    # Override UUID's __setattr__ method to make it mutable.
    super(UUID, self).__setattr__(name, value)

  def to_lexicographic_str(self):
    from kronos.utils.math import uuid_to_time
    dt = datetime.utcfromtimestamp(uuid_to_time(self))
    if dt.microsecond == 0:
      dt = dt.replace(microsecond=1)
    return '%s%s' % (dt, self.bytes)

  def __cmp__(self, other):
    if other is None:
      return 1
    if isinstance(other, StringTypes):
      try:
        other = UUID(other)
      except (ValueError, AttributeError):
        return 1
    if isinstance(other, UUID):
      return self._cmp_multiplier * cmp((self.time, self.bytes),
                                        (other.time, other.bytes))
    raise InvalidTimeUUIDComparison('Compared TimeUUID to type {0}'
                                    .format(type(other)))
