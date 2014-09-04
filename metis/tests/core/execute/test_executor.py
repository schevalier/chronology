import functools
import json
import random

from collections import defaultdict

from metis.conf import constants
from metis.core.query.aggregate import Avg
from metis.core.query.aggregate import Count
from metis.core.query.aggregate import GroupBy
from metis.core.query.aggregate import Max
from metis.core.query.aggregate import Min
from metis.core.query.aggregate import Sum
from metis.core.query.condition import Condition
from metis.core.query.stream import KronosStream
from metis.core.query.transform import Aggregate
from metis.core.query.transform import Filter
from metis.core.query.transform import Join
from metis.core.query.transform import Limit
from metis.core.query.transform import OrderBy
from metis.core.query.transform import Project
from metis.core.query.value import Add
from metis.core.query.value import Constant
from metis.core.query.value import Floor
from metis.core.query.value import Property
from metis.core.query.value import Subtract
from tests.conf.settings import EXECUTORS
from tests.server import MetisServerTestCase


def executor_test(function):
  @functools.wraps(function)
  def wrapper(self):
    for executor in EXECUTORS:
      self.executor = executor
      self.stream = '%s.%s' % (executor, function.func_name)
      function(self)
      self.kronos_client.delete(self.stream, 0, 10000)
    self.executor = None
  return wrapper


class ExecutorTestCase(MetisServerTestCase):
  '''
  Unit tests for all available `Executor.execute` methods.
  '''
  def validate_response(self, response):
    self.assertEqual(response.status_codes, 200)
    return map(json.loads, response.data.splitlines())

  @executor_test
  def test_kronos(self):
    events = self.query(KronosStream('http://localhost:9191',
                                     self.stream,
                                     0,
                                     1000).to_dict())
    self.assertEqual(len(events), 0)
    for i in xrange(25):
      self.kronos_client.put({
        self.stream: [{constants.TIMESTAMP_FIELD: random.randint(0, 999)}]
        })
    events = self.query(KronosStream('http://localhost:9191',
                                     self.stream,
                                     0,
                                     1000).to_dict())
    self.assertEqual(len(events), 25)
    for event in events:
      self.assertTrue(event[constants.TIMESTAMP_FIELD] >= 0)
      self.assertTrue(event[constants.TIMESTAMP_FIELD] < 1000)

  @executor_test
  def test_project(self):
    for i in xrange(25):
      self.kronos_client.put({
        self.stream: [{constants.TIMESTAMP_FIELD: random.randint(0, 999),
                       'i': i,
                       'i+1': i+1}]
        })
    events = self.query(Project(KronosStream('http://localhost:9191',
                                             self.stream,
                                             0,
                                             1000),
                                [Property('i', alias='I'),
                                 Constant(10, alias='const'),
                                 Add([Property('i'), Property('i+1'),
                                      Constant(5)],
                                     alias='func')],
                                merge=True).to_dict())
    self.assertEqual(len(events), 25)
    for event in events:
      self.assertEqual(event['i'], event['I'])
      self.assertEqual(event['const'], 10)
      self.assertEqual(event['func'], event['i'] * 2 + 6)
      self.assertEqual(event['i+1'], event['i'] + 1)
      self.assertTrue(event[constants.TIMESTAMP_FIELD] >= 0)
      self.assertTrue(event[constants.TIMESTAMP_FIELD] < 1000)

  @executor_test
  def test_filter(self):
    for i in xrange(2000):
      event = {constants.TIMESTAMP_FIELD: random.randint(0, 999),
                          'a': random.randint(0, 10),
                          'b': random.randint(50, 150),
                          'c': [random.randint(0, 20) for j in xrange(10)]}
      if random.randint(0, 100) > 50:
        event['d'] = 'iamlolcat'
      else:
        event['d'] = 'helloworld'
      self.kronos_client.put({self.stream: [event]})
    events = self.query(Filter(KronosStream('http://localhost:9191',
                                            self.stream,
                                            0,
                                            1000),
                               ((Condition(Condition.Op.GT,
                                           Property(constants.TIMESTAMP_FIELD),
                                           Constant(500)) |
                                 Condition(Condition.Op.LTE,
                                           Property('b'),
                                           Constant(100))) &
                                (Condition(Condition.Op.CONTAINS,
                                           Property('c'),
                                           Property('a')) |
                                 Condition(Condition.Op.REGEX,
                                           Property('d'),
                                           Constant('lolcat'))))).to_dict())
    self.assertTrue(len(events) > 0)
    self.assertTrue(len(events) < 2000)
    for event in events:
      self.assertTrue(event[constants.TIMESTAMP_FIELD] > 500 or
                      event['b'] <= 100)
      self.assertTrue(event['a'] in event['c'] or
                      'lolcat' in event['d'])
      self.assertTrue(event[constants.TIMESTAMP_FIELD] >= 0)
      self.assertTrue(event[constants.TIMESTAMP_FIELD] < 1000)

  @executor_test
  def test_order_by(self):
    for i in xrange(100):
      self.kronos_client.put({
        self.stream: [{constants.TIMESTAMP_FIELD: random.randint(0, 999),
                       'a': random.randint(0, 5),
                       'b': random.randint(1000, 10000)}]
        })
    # NOP projection to ensure events flow through Spark.
    events = self.query(Project(KronosStream('http://localhost:9191',
                                             self.stream,
                                             0,
                                             1000),
                                [Property('a', alias='a')],
                                merge=True).to_dict())
    # By default, should be ordered by time.
    self.assertEqual(len(events), 100)
    times = [event[constants.TIMESTAMP_FIELD] for event in events]
    self.assertEqual(times, sorted(times))

    events = self.query(OrderBy(KronosStream('http://localhost:9191',
                                             self.stream,
                                             0,
                                             1000),
                                [Property('a'), Property('b')]).to_dict())
    self.assertEqual(len(events), 100)
    a = b = -float('inf')
    for event in events:
      if a != event['a']:
        b = -float('inf')
      self.assertTrue(a <= event['a'])
      self.assertTrue(b <= event['b'])
      a = event['a']
      b = event['b']

  @executor_test
  def test_limit(self):
    for i in xrange(20):
      self.kronos_client.put({
        self.stream: [{constants.TIMESTAMP_FIELD: i}]
        })
    events = self.query(Limit(KronosStream('http://localhost:9191',
                                           self.stream,
                                           0,
                                           20),
                              10).to_dict())
    self.assertEqual(len(events), 10)
    for i, event in enumerate(events):
      self.assertEqual(event[constants.TIMESTAMP_FIELD], i)

  @executor_test
  def test_aggregate(self):
    sums = defaultdict(int)
    for i in xrange(200):
      a = random.randint(0, 2)
      self.kronos_client.put({
        self.stream: [{constants.TIMESTAMP_FIELD: i, 'a': a}]
        })
      sums[50 * (i / 50)] += a
    events = self.query(
      Aggregate(Project(KronosStream('http://localhost:9191',
                                     self.stream,
                                     0,
                                     1000),
                        [Floor([Property(constants.TIMESTAMP_FIELD),
                                Constant(50)],
                               alias=constants.TIMESTAMP_FIELD)],
                        merge=True),
                GroupBy(Property(constants.TIMESTAMP_FIELD,
                                 alias=constants.TIMESTAMP_FIELD)),
                [Count([], alias='count'),
                 Sum([Property('a')], alias='sum'),
                 Min([Property('a')], alias='min'),
                 Max([Property('a')], alias='max'),
                 Avg([Property('a')], alias='avg')]).to_dict())
    self.assertEqual(len(events), 200 / 50)
    for event in events:
      self.assertEqual(event[constants.TIMESTAMP_FIELD] % 50, 0)
      self.assertEqual(event['count'], 50)
      self.assertEqual(event['min'], 0)
      self.assertEqual(event['max'], 2)
      self.assertEqual(event['sum'], sums[event[constants.TIMESTAMP_FIELD]])
      self.assertTrue(event['avg'] * 50 > event['sum'] - 0.1)
      self.assertTrue(event['avg'] * 50 < event['sum'] + 0.1)

    events = self.query(
      Aggregate(KronosStream('http://localhost:9191',
                             self.stream,
                             0,
                             1000),
                GroupBy(Floor([Property(constants.TIMESTAMP_FIELD),
                               Constant(50)],
                              alias=constants.TIMESTAMP_FIELD)),
                [Count([], alias='count')]).to_dict())
    self.assertEqual(len(events), 200 / 50)

  @executor_test
  def test_join(self):
    for i in xrange(100):
      self.kronos_client.put({
        self.stream + '1': [{constants.TIMESTAMP_FIELD: i,
                             'a': random.randint(0, 2),
                             'b': random.randint(0, 5)}]
        })
    for i in xrange(100):
      self.kronos_client.put({
        self.stream + '2': [{constants.TIMESTAMP_FIELD: i,
                             'a': random.randint(0, 2),
                             'b': random.randint(0, 5)}]
        })
    events = self.query(Join(KronosStream('http://localhost:9191',
                                          self.stream + '1',
                                          0,
                                          200,
                                          alias='j1'),
                             KronosStream('http://localhost:9191',
                                          self.stream + '2',
                                          0,
                                          200),
                             (Condition(Condition.Op.EQ,
                                        Property('j1.a'),
                                        Property('right.a')) &
                              Condition(Condition.Op.GT,
                                        Property('j1.b'),
                                        Property('right.b')))).to_dict())
    self.assertTrue(len(events) > 0)
    for event in events:
      self.assertEqual(event['j1.a'], event['right.a'])
      self.assertTrue(event['j1.b'] > event['right.b'])
      self.assertEqual(set(event),
                       {'j1.%s' % constants.TIMESTAMP_FIELD,
                        'right.%s' % constants.TIMESTAMP_FIELD,
                        'j1.%s' % constants.ID_FIELD,
                        'right.%s' % constants.ID_FIELD,
                        'j1.a', 'right.a',
                        'j1.b', 'right.b'})

  @executor_test
  def test_join_eq(self):
    for i in xrange(200):
      self.kronos_client.put({
        self.stream + '1': [{constants.TIMESTAMP_FIELD: random.randint(0, 999),
                             'a': i,
                             'b': i + 1}]
        })
    for i in xrange(200):
      self.kronos_client.put({
        self.stream + '2': [{constants.TIMESTAMP_FIELD: random.randint(0, 999),
                             'a': i + 1,
                             'b': i + 2}]
        })

    # 1-1 join with property.
    events = self.query(Join(KronosStream('http://localhost:9191',
                                          self.stream + '1',
                                          0,
                                          1000),
                             KronosStream('http://localhost:9191',
                                          self.stream + '2',
                                          0,
                                          1000),
                             # left.a == right.b
                             Condition(Condition.Op.EQ,
                                       Property('left.b'),
                                       Property('right.a'))).to_dict())
    self.assertEqual(len(events), 200)
    for event in events:
      self.assertEqual(event['left.b'], event['right.a'])

    # 1-1 join with function.
    events = self.query(Join(KronosStream('http://localhost:9191',
                                          self.stream + '1',
                                          0,
                                          1000),
                             KronosStream('http://localhost:9191',
                                          self.stream + '2',
                                          0,
                                          1000),
                             # left.a == (right.a - 1)
                             Condition(Condition.Op.EQ,
                                       Property('left.a'),
                                       Subtract([Property('right.a'),
                                                 Constant(1)]))).to_dict())
    self.assertEqual(len(events), 200)
    for event in events:
      self.assertEqual(event['left.a'], event['right.a'] - 1)

    # 1-1 eqjoin with filtering.
    events = self.query(Join(KronosStream('http://localhost:9191',
                                          self.stream + '1',
                                          0,
                                          1000),
                             KronosStream('http://localhost:9191',
                                          self.stream + '2',
                                          0,
                                          1000),
                             (Condition(Condition.Op.EQ,
                                        Property('left.b'),
                                        Property('right.a')) &
                              Condition(Condition.Op.GT,
                                        Property('left.%s' %
                                                 constants.TIMESTAMP_FIELD),
                                        Add([Property('right.%s' %
                                                      constants.TIMESTAMP_FIELD),
                                             Constant(10)])))).to_dict())
    self.assertTrue(len(events) > 0)
    self.assertTrue(len(events) < 200)
    for event in events:
      self.assertEqual(event['left.b'], event['right.a'])
      self.assertTrue(event['left.%s' % constants.TIMESTAMP_FIELD] >
                      event['right.%s' % constants.TIMESTAMP_FIELD] + 10)
