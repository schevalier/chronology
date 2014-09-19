import itertools

from collections import defaultdict

from metis.core.execute.base import Executor
from metis.core.execute.utils import generate_filter
from metis.core.execute.utils import get_value


class PythonExecutor(Executor):
  def execute_kronos_stream(self, node):
    from pykronos import KronosClient

    client = KronosClient(node.host, blocking=True)
    return client.get(node.stream,
                      node.start_time,
                      node.end_time,
                      namespace=node.namespace)

  def execute_aggregate(self, node):
    groups = defaultdict(list)

    for event in self.execute(node.stream):
      key, event = node.group_func(event)
      groups[key].append(event)
    for key in groups:
      yield node.finalize_func(reduce(node.reduce_func, groups[key]))

  def execute_filter(self, node):
    return itertools.ifilter(generate_filter(node.condition),
                             self.execute(node.stream))

  def execute_join(self, node):
    left_alias = node.left.alias or 'left'
    right_alias = node.right.alias or 'right'
    _filter = generate_filter(node.condition)

    def merge(event1, event2):
      event = {}
      for key, value in event1.iteritems():
        event['%s.%s' % (left_alias, key)] = value
      for key, value in event2.iteritems():
        event['%s.%s' % (right_alias, key)] = value
      return event

    # TODO(usmanm): All joins are Cartesian for now. Add some eq-join
    # optimizations.
    right = list(self.execute(node.right))
    for event1 in self.execute(node.left):
      for event2 in right:
        event = merge(event1, event2)
        if not _filter(event):
          continue
        yield event

  def execute_limit(self, node):
    return itertools.islice(self.execute(node.stream), node.limit)

  def execute_order_by(self, node):
    events = sorted(self.execute(node.stream),
                    key=lambda e: tuple(get_value(e, field)
                                        for field in node.fields))
    for i in (xrange(len(events) - 1, -1, -1)
              if node.order == node.ResultOrder.DESCENDING else
              xrange(len(events))):
      yield events[i]

  def execute_project(self, node):
    return itertools.imap(node.map_func, self.execute(node.stream))
