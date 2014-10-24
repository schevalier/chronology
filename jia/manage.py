#!/usr/bin/env python
from commands.runserver import Server
from flask.ext.script import Manager
from flask.ext.migrate import MigrateCommand
from jia import config

manager = Manager(config)
manager.add_option('--config', dest='settings_file', required=False)
manager.add_command('runserver', Server())
manager.add_command('db', MigrateCommand)

if __name__ == '__main__':
  manager.run()
