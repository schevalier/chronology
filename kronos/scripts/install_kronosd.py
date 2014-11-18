#!/usr/bin/python

import errno
import grp
import os
import pwd
import re
import shutil
import tempfile

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        os.path.pardir))
LOG_DIR_RE = re.compile(r"'log_directory': '[^\']+'", re.I)
LIB_DIR = '/usr/lib/kronos'
LOG_DIR = '/var/log/kronos'
RUN_DIR = '/var/run/kronos'
TMP_DIR = tempfile.gettempdir()
UWSGI_VERSION = '2.0.5.1'
SERVING_MODES = [('all', '8150'), # Kronos serving mode and uWSGI port.
                 ('collector', '8151'),
                 ('readonly', '8152')]

def run_cmd(cmd):
  print '> %s' % cmd
  assert os.system(cmd) == 0


def safe_mkdir(path):
  print '> mkdir %s' % path
  try:
    os.makedirs(path)
  except OSError as e:
    if e.errno != errno.EEXIST:
      raise e


def create_user_and_group():
  print 'Creating kronos user and group accounts...'
  try:
    pwd.getpwnam('kronos')
  except KeyError:
    run_cmd('useradd kronos')
  try:
    grp.getgrnam('kronos')
  except KeyError:
    run_cmd('groupadd kronos')
  print 'done.'


def make_dirs():
  print 'Creating directories...'
  safe_mkdir('/etc/kronos')
  safe_mkdir(LOG_DIR)
  safe_mkdir(RUN_DIR)
  safe_mkdir(LIB_DIR)
  print 'done.'


def copy_files():
  print 'Copying configuration and init.d script files...'

  uwsgi_file_path = os.path.join(BASE_DIR, 'scripts/uwsgi.ini')
  uwsgi_tmp_file_path = os.path.join(BASE_DIR, 'scripts/uwsgi.ini.tmp')
  kronosd_file_path = os.path.join(BASE_DIR, 'scripts/kronosd.init.d')

  for serving_mode, port in SERVING_MODES:
    uwsgi_new_file_path = os.path.join(
      BASE_DIR, 'scripts/uwsgi-{}.ini'.format(serving_mode))
    kronosd_new_file_path = os.path.join(
      BASE_DIR, 'scripts/kronosd-{}.init.d'.format(serving_mode))
    os.system('sed -e "s/__SERVINGMODE__/{serving_mode}/" {uwsgi} > {uwsgi_tmp}'
              .format(serving_mode=serving_mode,
                      uwsgi=uwsgi_file_path,
                      uwsgi_tmp=uwsgi_tmp_file_path))
    os.system('sed -e "s/__SOCKET__/{socket}/" {uwsgi_tmp} > {uwsgi_new}'
              .format(socket=port,
                      serving_mode=serving_mode,
                      uwsgi_tmp=uwsgi_tmp_file_path,
                      uwsgi_new=uwsgi_new_file_path))

    os.system('sed -e "s/__SERVINGMODE__/{serving_mode}/" {kronosd} > '
              '{kronosd_new}'.format(serving_mode=serving_mode,
                                     kronosd=kronosd_file_path,
                                     kronosd_new=kronosd_new_file_path))

    shutil.copy(uwsgi_new_file_path, '/etc/kronos')
    shutil.copymode(kronosd_file_path, kronosd_new_file_path)
    shutil.copy(kronosd_new_file_path,
                '/etc/init.d/kronos-{}'.format(serving_mode))
  os.remove('scripts/uwsgi.ini.tmp')
  print 'done.'


def install_uwsgi():
  print 'Compiling uWSGI and copying it to the lib directory...'
  cwd = os.getcwd()
  uwsgi_dir = LIB_DIR + '/uwsgi'
  shutil.rmtree(uwsgi_dir, ignore_errors=True)
  safe_mkdir(uwsgi_dir)
  tmp_dir = '%s/uwsgi-%s' % (TMP_DIR, UWSGI_VERSION)
  os.chdir(TMP_DIR)
  run_cmd('wget https://github.com/unbit/uwsgi/archive/%s.tar.gz' %
          UWSGI_VERSION)
  run_cmd('tar xvzf %s.tar.gz' % UWSGI_VERSION)
  os.unlink('%s.tar.gz' % UWSGI_VERSION)
  os.chdir(tmp_dir)
  run_cmd('make')
  run_cmd('make plugin.transformation_chunked')
  run_cmd('make plugin.transformation_gzip')
  # Only copy the compiled binary + .so files for needed plugins.
  for name in ('uwsgi',
               'transformation_chunked_plugin.so',
               'transformation_gzip_plugin.so'):
    shutil.copy(name, '%s/%s' % (uwsgi_dir, name))
  run_cmd('chown -R kronos:kronos %s' % uwsgi_dir)
  os.chdir(cwd)
  shutil.rmtree(tmp_dir, ignore_errors=True)
  print 'done.'


def install_kronosd():
  create_user_and_group()
  make_dirs()
  install_uwsgi()
  copy_files()


if __name__ == '__main__':
  install_kronosd()
