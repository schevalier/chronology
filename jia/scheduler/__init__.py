from __future__ import absolute_import

import os
from flask import Flask
from flask.ext.migrate import Migrate
from flask.ext.sqlalchemy import SQLAlchemy
from time import sleep
from threading import Lock

db = SQLAlchemy()

_APP = None

def get_app(settings_file=None):
  """Get scheduler app singleton

  The app configuration is performed when the function is run for the first
  time.
  
  Because the scheduler is a threaded enviroment, it is important that this
  function be thread-safe. The scheduler instance is not created until the
  `commands/runscheduler.py` script is executed, and this function is first
  invoked by the scheduler.py management script. In other words, this function
  is guaranteed to run to completion (by the management script) before the
  scheduler thread is spawned. Should that ever change, locks would need to be
  added here.
  """
  global _APP
  if _APP:
    return _APP
  _APP = Flask(__name__)
  db.init_app(_APP)
  migrate = Migrate(_APP, db, directory='scheduler/migrations')

  _APP.config.from_pyfile('../jia/conf/default_settings.py')
  if settings_file:
    if not settings_file.startswith('/'):
      settings_file = os.path.join(os.pardir, settings_file)
    _APP.config.from_pyfile(settings_file, silent=True)
    
  _APP.config.update(PORT=_APP.config['SCHEDULER_PORT'])
  _APP.config.update(SQLALCHEMY_DATABASE_URI=_APP.config['SCHEDULER_DATABASE_URI'])

  _APP.secret_key = _APP.config['SECRET_KEY']

  from scheduler.views import scheduler
  _APP.register_blueprint(scheduler)

  return _APP
