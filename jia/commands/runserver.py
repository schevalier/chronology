import datetime
import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import jia
import os
import werkzeug.serving

from flask.ext.script import Command 
from flask.ext.script import Option


class Server(Command):
  """Jia HTTP server"""

  option_list = (
    Option('--port', type=int, dest='port', help='Port to listen on.'),
  )

  def log_info(self, host, port):
    return """
      %(date)s
      Starting jia server at http://%(host)s:%(port)s/
      Quit the server with CONTROL-C.
    """ % {
        'date': datetime.datetime.now().strftime("%B %d, %Y - %H:%M:%S"),
        'host': host,
        'port': port,
      }

  def __call__(self, app, port):
    if port:
      app.config['PORT'] = port

    # Show server info on initial run (not on reload)
    if not os.environ.get('WERKZEUG_RUN_MAIN'):
      print self.log_info(app.config['HOST'], app.config['PORT'])

    werkzeug.serving.run_with_reloader(
      lambda: gevent.pywsgi.WSGIServer((app.config['HOST'],
                                        app.config['PORT']),
                                       app).serve_forever())

