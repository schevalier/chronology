DEBUG = True

# Port to listen for request on.
PORT = 8152

# All enabled executors.
EXECUTORS = ['metis.core.execute.python.PythonExecutor']

# The default executor to use, if none is specified in the request.
DEFAULT_EXECUTOR = 'metis.core.execute.python.PythonExecutor'

DATA_SOURCES = {
  'kronos': {
    'type': 'metis.core.data.kronos.KronosStream',
    'pretty_name': 'Kronos',
    'url': 'http://localhost:8150'
  }
}
