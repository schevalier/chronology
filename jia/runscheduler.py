#!/usr/bin/env python

"""Starting point for the Jia Scheduler

Never replace this runscheduler.py script with a WSGI server that serves
from multiple threads/processes. Because each Flask app would receive its own
instance of Scheduler (with its own task queue), this application is not
parallelizable. Running multiple Flask instances will result in creating
duplicate running tasks, especially after restarting the server.
"""
import datetime
import gevent
import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import os
import werkzeug.serving

from argparse import ArgumentParser
from scheduler import app
from scheduler.scheduler import Scheduler


def log_info(host, port):
  return """
    %(date)s
    Starting scheduler server at http://%(host)s:%(port)s/
    Quit the server with CONTROL-C.
  """ % {
      'date': datetime.datetime.now().strftime("%B %d, %Y - %H:%M:%S"),
      'host': host,
      'port': port,
    }


if __name__ == '__main__':
  parser = ArgumentParser(description='HTTP scheduler')
  parser.add_argument('--port', type=int, help='Port to listen on.')
  parser.add_argument('--config', help='Path of config file to use.')
  args = parser.parse_args()
  for key, value in args.__dict__.items():
    if value is not None:
      if key == 'config':
        app.config.from_pyfile(os.path.join(os.pardir, args.config),
                               silent=True)
      elif key == 'port':
        app.config['SCHEDULER_PORT'] = value
      else:
        app.config[key.upper()] = value

  # Show server info on initial run (not on reload)
  if not os.environ.get('WERKZEUG_RUN_MAIN'):
    print log_info(app.config['SCHEDULER_HOST'], app.config['SCHEDULER_PORT'])

  app.scheduler = Scheduler()
  werkzeug.serving.run_with_reloader(
    lambda: gevent.pywsgi.WSGIServer((app.config['SCHEDULER_HOST'],
                                      app.config['SCHEDULER_PORT']),
                                     app).serve_forever())
