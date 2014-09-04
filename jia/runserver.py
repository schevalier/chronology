#!/usr/bin/env python

import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import werkzeug.serving

from argparse import ArgumentParser
from jia import app

if __name__ == '__main__':
  parser = ArgumentParser(description='Jia HTTP server.')
  parser.add_argument('--port', type=int, help='Port to listen on.')
  parser.add_argument('--config', help='Path of config file to use.')

  args = parser.parse_args()
  for key, value in args.__dict__.items():
    if value is not None:
      app.config[key.upper()] = value

  werkzeug.serving.run_with_reloader(
    lambda: gevent.pywsgi.WSGIServer(('0.0.0.0', app.config['PORT']),
                                     app).serve_forever())

