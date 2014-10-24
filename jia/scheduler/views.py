from __future__ import absolute_import

import binascii
import json
import os

from flask import Blueprint
from flask import current_app
from flask import request
from scheduler.models import Task
from scheduler.decorators import token_protected_endpoint

scheduler = Blueprint('scheduler', __name__)


@scheduler.route("/schedule", methods=['POST'])
@token_protected_endpoint
def schedule():
  """HTTP endpoint for scheduling tasks

  If a task with the same code already exists, the one with the shorter
  interval will be made active.
  """
  code = request.form['code']
  interval = int(request.form['interval'])

  task_id = binascii.b2a_hex(os.urandom(5))
  new_task = Task(id=task_id)
  new_task.active = True
  new_task.code = code
  new_task.interval = interval

  # TODO(derek): Assert there is only one other_task
  other_task = Task.query.filter_by(code=code, active=True).first()

  if other_task:
    if other_task.interval <= new_task.interval:
      new_task.active = False
    else:
      other_task.active = False
      other_task.save()
      current_app.scheduler.cancel(other_task.id)

  if new_task.active:
    print current_app.scheduler.schedule
    current_app.scheduler.schedule({
      'id': task_id,
      'code': new_task.code,
      'interval': new_task.interval
    })

  new_task.save()

  return json.dumps({
    'status': 'success',
    'id': task_id,
  })


@scheduler.route("/cancel", methods=['POST'])
@token_protected_endpoint
def cancel():
  """HTTP endpoint for canceling tasks

  If an active task is cancelled, an inactive task with the same code and the
  smallest interval will be activated if it exists.
  """
  task_id = request.form['id']
  task = Task.query.get(task_id)

  if not task:
    return json.dumps({
      'status': 'success',
      'id': None,
    })

  task.delete()

  if task.active:
    current_app.scheduler.cancel(task_id)

    code = task.code
    other_task = Task.query.filter_by(code=code).order_by('interval').first()
    if other_task:
      other_task.active = True
      other_task.save()
      current_app.scheduler.schedule({
        'id': other_task.id,
        'code': other_task.code,
        'interval': other_task.interval
      })

  return json.dumps({
    'status': 'success',
    'id': task_id,
  })
