import json
from jia import db


read_permissions = db.Table('read_permissions',
  db.Column('user_id', db.Integer, db.ForeignKey('user.id')),
  db.Column('board_id', db.String, db.ForeignKey('board.id'))
)

write_permissions = db.Table('write_permissions',
  db.Column('user_id', db.Integer, db.ForeignKey('user.id')),
  db.Column('board_id', db.String, db.ForeignKey('board.id'))
)

ownership_permissions = db.Table('ownership_permissions',
  db.Column('user_id', db.Integer, db.ForeignKey('user.id')),
  db.Column('board_id', db.String, db.ForeignKey('board.id'))
)


class User(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  email = db.Column(db.String)
  name = db.Column(db.String)
  picture = db.Column(db.String)
  locale = db.Column(db.String)
  hd = db.Column(db.String)  # Google Apps organization domain


class Board(db.Model):
  class PanelSource(object):
    PYCODE = 'pycode'

  class PanelDisplay(object):
    TIMESERIES = 'timeseries'
    TABLE = 'table'

  id = db.Column(db.String, primary_key=True)

  # Permissions
  public_can_read = db.Column(db.Boolean)
  public_can_write = db.Column(db.Boolean)
  readers = db.relationship('User', secondary=read_permissions,
                            backref=db.backref('readable_boards',
                                               lazy='dynamic'))
  writers = db.relationship('User', secondary=write_permissions,
                            backref=db.backref('writable_boards',
                                               lazy='dynamic'))
  owners = db.relationship('User', secondary=ownership_permissions,
                           backref=db.backref('owned_boards', lazy='dynamic'))

  # JSON-encoded description of the board of the form { '__version__':
  # 1, ...}, where ... is specified in `Board.json`.
  #
  # `__version__` is the serialization version of the board.  If, in
  # the future, we change the serialization format of boards, we'll be
  # able to read and migrate previously serialized boards.
  board_data = db.Column(db.String)

  def save(self):
    db.session.add(self)
    db.session.commit()

  def delete(self):
    db.session.delete(self)
    db.session.commit()

  def json(self):
    """A JSON-encoded description of this board.

    Format:
    {'id': board_id,
     'title': 'The title of the board',
     'panels': [{
       'title': 'The title of the panel'
       'data_source': {
         'source_type': PanelSource.TYPE,
         'refresh_seconds': 600,
         ...source_specific_details...
       },
       'display': {
         'display_type': PanelDisplay.TYPE,
         ...display_specific_details...
       }, ...]}
    """
    if self.board_data:
      board_dict = json.loads(self.board_data)
      board_dict['id'] = self.id
      del board_dict['__version__']
    else:
      board_dict = {
        'id': self.id,
        'title': '',
        'panels': []
      }
    return board_dict
    """    pycode = self.pycodes.first() or PyCode()
    return {'id': self.id,
            'pycode': pycode.json()}
            """
  def set_board_data(self, board_dict):
    assert board_dict['id'] == self.id
    del board_dict['id']
    self.board_data = json.dumps(dict(board_dict.items() +
                                      [('__version__', 1)]))
