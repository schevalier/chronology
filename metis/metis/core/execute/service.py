from importlib import import_module

from metis import app
from metis.common.lazy import LazyObjectMetaclass
from metis.core.query.plan import parse


class ExecutorService(object):
  __metaclass__ = LazyObjectMetaclass

  def __init__(self):
    self.executors = {}
    self.default_executor = app.config['DEFAULT_EXECUTOR']
    
    for executor in app.config['EXECUTORS']:
      executor_path = 'metis.core.execute.%s' % executor
      executor_module, executor_cls = executor_path.rsplit('.', 1)
      executor_module = import_module(executor_module)
      executor_cls = getattr(executor_module, executor_cls)
      self.executors[executor] = executor_cls()

    assert self.default_executor in self.executors

  def execute_plan(self, plan, executor=None):
    if executor is None:
      executor = self.default_executor
    executor = self.executors[executor]
    return executor.finalize(executor.execute(parse(plan)))


service = ExecutorService()
