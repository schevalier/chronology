class TwoWayDict(dict):
  def __init__(self, *args, **kwargs):
    other = kwargs.pop('__other__', None)
    super(TwoWayDict, self).__init__(*args, **kwargs)
    if other is None:
      self.other = TwoWayDict(((value, key) for key, value in self.iteritems()),
                              __other__=self)
    else:
      self.other = other
    assert isinstance(self.other, TwoWayDict)

  def get_inverse_dict(self):
    return self.other

  def __setitem__(self, key, value):
    if key in self:
      # Setting key => value, but we already have key => self[key].
      # which implies we have self[key] => key in other. Remove that, otherwise
      # the bijection property will be lost.
      super(TwoWayDict, self.other).__delitem__(self[key])

    if value in self.other:
      # We already have value => self.other[value] in other, which implies we
      # have self.other[value] => value in self. Remove that, otherwise the
      # bijection property will be lost.
      super(TwoWayDict, self).__delitem__(self.other[value])

    # Set mappings in both directions.
    super(TwoWayDict, self).__setitem__(key, value)
    super(TwoWayDict, self.other).__setitem__(value, key)

  def __delitem__(self, key):
    # Delete mapping from both dicts.
    super(TwoWayDict, self.other).__delitem__(self[key])
    super(TwoWayDict, self).__delitem__(key)
