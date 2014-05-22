storage = {
  's3': {
    'backend': 's3.S3Storage',
  }
}

default_namespace = 'kronos'

_default_stream_configuration = {
  '': {
    'backends': {
      's3': None
      },
    'read_backend': 's3'
    }
  }
