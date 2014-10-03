from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from flask_sslify import SSLify

class JiaServer(Flask):
  jinja_options = Flask.jinja_options.copy()
  jinja_options.update(dict(
      variable_start_string='<%',
      variable_end_string='%>',
  ))

app = JiaServer(__name__)
app.config.from_object('jia.conf.default_settings')
app.secret_key = app.config['SECRET_KEY']
db = SQLAlchemy(app)

if app.config['FORCE_SSL']:
  sslify = SSLify(app)

import jia.models
import jia.views  # noqa

# Create tables in sqlite3 db if not present.
db.create_all()
