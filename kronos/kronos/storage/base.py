from timeuuid import TimeUUID

from kronos.conf.constants import MAX_LIMIT
from kronos.conf.constants import ResultOrder
from kronos.utils.uuid import uuid_from_kronos_time
from kronos.utils.uuid import UUIDType


def _get_timeuuid(ktime, _id, _type=UUIDType.LOWEST):
  if not _id:
    _id = uuid_from_kronos_time(ktime, _type=_type)
  else:
    _id = TimeUUID(_id)
  return _id


class BaseStorage(object):
  # All subclasses must define `SETTINGS_VALIDATORS` mapping option names to a
  # function that takes a value for that option and returns True if it is valid
  # and False otherwise. For example, if a subclass takes one option called
  # `max_size` that should be a nonnegative integer, we would have:
  # SETTINGS_VALIDATORS = { 'max_size': lambda x: int(x) >= 0 }
  SETTINGS_VALIDATORS = {}

  def __init__(self, name, namespaces, **settings):
    """
    Subclasses can assume that `settings` only contains keys that are also in
    `SETTINGS_VALIDATORS` and that their values are valid.
    """
    self.name = name
    for setting in self.__class__.SETTINGS_VALIDATORS:
      setattr(self, setting, settings[setting])
      assert self.__class__.SETTINGS_VALIDATORS[setting](getattr(self, setting))
    self._settings = settings
    self.namespaces = namespaces

  def is_alive(self):
    raise NotImplementedError('Must implement `is_alive` method for %s' %
                              self.__class__.__name__)

  def insert(self, namespace, stream, events, configuration):
    self._insert(namespace, stream, events, configuration)

  def _insert(self, namespace, stream, events, configuration):
    raise NotImplementedError('Must implement `_insert` method for %s' %
                              self.__class__.__name__)

  def delete(self, namespace, stream, start_time, end_time, start_id,
             end_id, configuration):
    start_id = _get_timeuuid(start_time, start_id)
    end_id = _get_timeuuid(end_time, end_id)
    if start_id >= end_id:
      return 0, []
    return self._delete(namespace, stream, start_id, end_id, configuration)

  def _delete(self, stream, start_id, end_id, configuration, namespace):
    raise NotImplementedError('Must implement `_delete` method for %s' %
                              self.__class__.__name__)

  def retrieve(self, namespace, stream, start_time, end_time, start_id,
               end_id, configuration, order=ResultOrder.ASCENDING,
               limit=MAX_LIMIT):
    """
    Retrieves all the events for `stream` from `start_time` (inclusive) till
    `end_time` (inclusive). Alternatively to `start_time`, `start_id` can be
    provided, and then all events from `start_id` (exclusive) till `end_time`
    (inclusive) are returned. `start_id` should be used in cases when the client
    got disconnected from the server before all the events in the requested
    time window had been returned. `order` can be one of ResultOrder.ASCENDING
    or ResultOrder.DESCENDING.

    Returns an iterator over all JSON serialized (strings) events.
    """
    if order == ResultOrder.DESCENDING:
      _type = UUIDType.HIGHEST
    else:
      _type = UUIDType.LOWEST
    start_id = _get_timeuuid(start_time , start_id, _type)
    end_id = _get_timeuuid(end_time, end_id, _type)
    if start_id >= end_id:
      return []
    return self._retrieve(namespace, stream, start_id, end_id, order, limit,
                          configuration)

  def _retrieve(self, namespace, stream, start_id, end_id, order, limit,
                configuration):
    raise NotImplementedError('Must implement `_retrieve` method for %s.' %
                              self.__class__.__name__)

  def streams(self, namespace):
    return self._streams(namespace)

  def _streams(self, namespace):
    raise NotImplementedError('Must implement `_streams` method for %s' %
                              self.__class__.__name__)

  def _clear(self):
    """
      helper method used to clear the db during testing
    """
    raise NotImplementedError('Must implement `_clear` method for %s' %
                              self.__class__.__name__)

  def stop(self):
    """ The backend will be removed from the router. Stop any background
    activity. """
    pass
