import json

from copy import deepcopy

from metis.core.execute.utils import cast_to_number
from metis.core.execute.utils import get_value
from metis.core.query import ExecutableNode
from metis.core.query.aggregate import Aggregator
from metis.core.query.aggregate import GroupBy
from metis.core.query.condition import Condition
from metis.core.query.stream import Stream
from metis.core.query.value import Value
from metis.utils.enum import Enum


def _parse_stream_or_transform(_dict):
  if not _dict:
    return None
  typ = _dict['type']
  if typ in Stream.Type.values():
    return Stream.parse(_dict)
  assert typ in Transform.Type.values()
  return Transform.parse(_dict)


class Transform(ExecutableNode):
  class Type(Enum):
    PROJECT = 'project'
    FILTER = 'filter'
    ORDER_BY = 'order_by'
    LIMIT = 'limit'
    AGGREGATE = 'aggregate'
    JOIN = 'join'

  def __init__(self, alias=None):
    self.alias = alias

  def validate(self):
    return self.type in Transform.Type.values()

  @classmethod
  def parse(self, _dict):
    typ = _dict['type']

    if typ in Stream.Type.values():
      return Stream.parse(_dict)

    assert typ in Transform.Type.values()
    del _dict['type']

    if 'stream' in _dict:
      _dict['stream'] = _parse_stream_or_transform(_dict['stream'])

    if typ == Transform.Type.PROJECT:
      return Project.parse(_dict)
    if typ == Transform.Type.FILTER:
      return Filter.parse(_dict)
    if typ == Transform.Type.ORDER_BY:
      return OrderBy.parse(_dict)
    if typ == Transform.Type.LIMIT:
      return Limit.parse(_dict)
    if typ == Transform.Type.AGGREGATE:
      return Aggregate.parse(_dict)
    if typ == Transform.Type.JOIN:
      return Join.parse(_dict)


class Project(Transform):
  def __init__(self, stream, fields, merge=False, **kwargs):
    self.type = Transform.Type.PROJECT
    self.stream = stream
    self.fields = fields
    self.merge = merge
    super(Project, self).__init__(**kwargs)

  def map_func(self, event):
    if self.merge:
      new_event = event
    else:
      new_event = {}
    for field in self.fields:
      new_event[field.alias] = get_value(event, field)
    return new_event

  @classmethod
  def parse(self, _dict, **kwargs):
    _dict['fields'] = map(Value.parse, _dict['fields'])
    return Project(**_dict)


class Filter(Transform):
  def __init__(self, stream, condition, **kwargs):
    self.type = Transform.Type.FILTER
    self.stream = stream
    self.condition = condition
    super(Filter, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict):
    _dict['condition'] = Condition.parse(_dict['condition'])
    return Filter(**_dict)


class OrderBy(Transform):
  class ResultOrder(Enum):
    ASCENDING = 'ascending'
    DESCENDING = 'descending'

  def __init__(self, stream, fields, order=ResultOrder.ASCENDING, **kwargs):
    self.type = Transform.Type.ORDER_BY
    self.stream = stream
    self.fields = fields
    self.order = order 
    super(OrderBy, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict):
    _dict['fields'] = map(Value.parse, _dict['fields'])
    return OrderBy(**_dict)


class Limit(Transform):
  def __init__(self, stream, limit, **kwargs):
    self.type = Transform.Type.LIMIT
    self.stream = stream
    self.limit = limit
    super(Limit, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict):
    return Limit(**_dict)


class Aggregate(Transform):
  def __init__(self, stream, group_by, aggregates, **kwargs):
    self.type = Transform.Type.AGGREGATE
    self.stream = stream
    self.aggregates = aggregates
    self.group_by = group_by
    super(Aggregate, self).__init__(**kwargs)

  def group_func(self, event):
    new_event = {value.alias: get_value(event, value)
                 for value in self.group_by.values}
    key = json.dumps(new_event, sort_keys=True)
    for aggregate in self.aggregates:
      arguments = aggregate.arguments
      if aggregate.op == Aggregator.Op.COUNT:
        if not len(arguments):
          value = 1
        else:
          value = 0 if get_value(event, arguments[0]) is None else 1
      elif aggregate.op == Aggregator.Op.SUM:
        value = cast_to_number(get_value(event, arguments[0]), 0)
      elif aggregate.op == Aggregator.Op.MIN:
        value = cast_to_number(get_value(event, arguments[0]), float('inf'))
      elif aggregate.op == Aggregator.Op.MAX:
        value = cast_to_number(get_value(event, arguments[0]), -float('inf'))
      elif aggregate.op == Aggregator.Op.AVG:
        value = cast_to_number(get_value(event, arguments[0]), None)
        if value is None:
          value = (0, 0)
        else:
          value = (value, 1)
      new_event[aggregate.alias] = value
    return key, new_event

  def reduce_func(self, event1, event2):
    event = deepcopy(event1)
    for aggregate in self.aggregates:
      alias = aggregate.alias
      if aggregate.op in (Aggregator.Op.COUNT, Aggregator.Op.SUM):
        value = event1[alias] + event2[alias]
      elif aggregate.op == Aggregator.Op.MIN:
        value = min(event1[alias], event2[alias])
      elif aggregate.op == Aggregator.Op.MAX:
        value = max(event1[alias], event2[alias])
      elif aggregate.op == Aggregator.Op.AVG:
        value = (event1[alias][0] + event2[alias][0],
                 event1[alias][1] + event2[alias][1])
      event[alias] = value
    return event

  def finalize_func(self, event):
    event = deepcopy(event)
    for aggregate in self.aggregates:
      if aggregate.op == Aggregator.Op.AVG:
        alias = aggregate.alias
        value = event[alias]
        if not value[1]:
          event[alias] = None
        else:
          event[alias] = value[0] / float(value[1])
    return event

  @classmethod
  def parse(self, _dict):
    _dict['aggregates'] = map(Aggregator.parse, _dict['aggregates'])
    _dict['group_by'] = GroupBy.parse(_dict['group_by'])
    return Aggregate(**_dict)


class Join(Transform):
  def __init__(self, left, right, condition, **kwargs):
    self.type = Transform.Type.JOIN
    self.left = left
    self.right = right
    self.condition = condition
    super(Join, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict):
    _dict['left'] = _parse_stream_or_transform(_dict['left'])
    _dict['right'] = _parse_stream_or_transform(_dict['right'])
    _dict['condition'] = Condition.parse(_dict['condition'])
    return Join(**_dict)
