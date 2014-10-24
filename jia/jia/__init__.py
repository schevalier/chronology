import os
from flask import Flask
from flask.ext.migrate import Migrate
from flask.ext.sqlalchemy import SQLAlchemy
from flask_sslify import SSLify

class JiaServer(Flask):
  jinja_options = Flask.jinja_options.copy()
  jinja_options.update(dict(
      variable_start_string='<%',
      variable_end_string='%>',
  ))

db = SQLAlchemy()

def config(settings_file=None):
  app = JiaServer(__name__)
  db.init_app(app)
  migrate = Migrate(app, db, directory='jia/migrations')

  app.config.from_object('jia.conf.default_settings')

  if settings_file:
    if not settings_file.startswith('/'):
      settings_file = os.path.join(os.pardir, settings_file)
    app.config.from_pyfile(settings_file, silent=True)

  app.secret_key = app.config['SECRET_KEY']

  if app.config['FORCE_SSL']:
    sslify = SSLify(app)

  from jia.views import app as app_blueprint
  from jia.auth import auth
  app.register_blueprint(app_blueprint)
  app.register_blueprint(auth)

  return app
