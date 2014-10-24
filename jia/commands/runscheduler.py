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

from flask import current_app
from flask.ext.script import Command
from flask.ext.script import Option
from scheduler.scheduler import Scheduler


class SchedulerServer(Command):
  """Jia HTTP task scheduler"""

  option_list = (
    Option('--port', type=int, dest='port', help='Port to listen on.'),
  )

  def log_info(self, host, port):
    return """
      %(date)s
      Starting scheduler server at http://%(host)s:%(port)s/
      Quit the server with CONTROL-C.
    """ % {
        'date': datetime.datetime.now().strftime("%B %d, %Y - %H:%M:%S"),
        'host': host,
        'port': port,
      }

  def __call__(self, app, port):
    if port:
      app.config['SCHEDULER_PORT'] = value
        
    # Show server info on initial run (not on reload)
    if not os.environ.get('WERKZEUG_RUN_MAIN'):
      print self.log_info(app.config['SCHEDULER_HOST'],
                          app.config['SCHEDULER_PORT'])

    app.scheduler = Scheduler()
    werkzeug.serving.run_with_reloader(
      lambda: gevent.pywsgi.WSGIServer((app.config['SCHEDULER_HOST'],
                                        app.config['SCHEDULER_PORT']),
                                       app).serve_forever())
