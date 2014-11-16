import time

from kronos.common.json_schema import get_schema_type
from kronos.common.json_schema import NullType
from kronos.common.time import epoch_time_to_kronos_time
from kronos.conf.constants import ResultOrder
from kronos.core import marshal
from kronos.storage.router import router


def infer_schema(namespace, stream):
  now = epoch_time_to_kronos_time(time.time())
  backend, configuration = router.backend_to_retrieve(namespace, stream)
  events = backend.retrieve(namespace, stream, 0, now, None, None,
                            configuration, order=ResultOrder.DESCENDING,
                            limit=100)
  schema_type = NullType()
  for event in events:
    schema_type = schema_type.combine(get_schema_type(marshal.loads(event)))
  schema = schema_type.to_dict()
  schema['$schema'] = 'http://json-schema.org/draft-04/schema'
  return schema
