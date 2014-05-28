storage = {
  's3': {
    'backend': 's3.S3Storage',
    'aws_access_key_id': 'AKIAJ3OSEQKRDHH6VLSA',
    'aws_secret_access_key': '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',
    'bucket_name': 'kronos_test',
    'flush_interval': 5 * 60,
    'local_dir': './tmp'
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
