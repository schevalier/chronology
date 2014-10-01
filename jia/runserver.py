#!/usr/bin/env python

import datetime
import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import os
import werkzeug.serving

from argparse import ArgumentParser
from jia import app


def log_info(host, port):
  return """
    %(date)s
    Starting jia server at http://%(host)s:%(port)s/
    Quit the server with CONTROL-C.
  """ % {
      'date': datetime.datetime.now().strftime("%B %d, %Y - %H:%M:%S"),
      'host': host,
      'port': port,
    }


if __name__ == '__main__':
  parser = ArgumentParser(description='Jia HTTP server.')
  parser.add_argument('--port', type=int, help='Port to listen on.')
  parser.add_argument('--config', help='Path of config file to use.')
  args = parser.parse_args()
  for key, value in args.__dict__.items():
    if value is not None:
      if key == 'config':
        app.config.from_pyfile(os.path.join(os.pardir, args.config),
                               silent=True)
      else:
        app.config[key.upper()] = value

  # Show server info on initial run (not on reload)
  if not os.environ.get('WERKZEUG_RUN_MAIN'):
    print log_info(app.config['HOST'], app.config['PORT'])

  werkzeug.serving.run_with_reloader(
    lambda: gevent.pywsgi.WSGIServer((app.config['HOST'], app.config['PORT']),
                                     app).serve_forever())
