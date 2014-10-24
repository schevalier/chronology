#!/usr/bin/env python
from commands.runscheduler import SchedulerServer
from flask.ext.script import Manager
from flask.ext.migrate import MigrateCommand
from scheduler import get_app

manager = Manager(get_app)
manager.add_option('--config', dest='settings_file', required=False)
manager.add_command('runserver', SchedulerServer())
manager.add_command('db', MigrateCommand)

if __name__ == '__main__':
  manager.run()
