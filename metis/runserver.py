#!/usr/bin/python
import os

from argparse import ArgumentParser

from metis import app


if __name__ == '__main__':
  parser = ArgumentParser(description='Metis HTTP server.')
  parser.add_argument('--debug', action='store_const', const=True,
                      help='Debug mode?')
  parser.add_argument('--reload', action='store_true', help='Auto-reload?')
  parser.add_argument('--port', help='Port to listen on.')
  parser.add_argument('--config', help='Path of config file to use.')
  args = parser.parse_args()

  if args.config:
    app.config.from_pyfile(os.path.join(os.pardir, args.config))

  if args.debug is not None:
    app.config['DEBUG'] = args.debug
  if args.port is not None:
    port = int(args.port)
    app.config['PORT'] = port

  app.run(host='0.0.0.0', port=app.config['PORT'], debug=app.config['DEBUG'],
          use_reloader=args.reload)
