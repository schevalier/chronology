import types


class Type(object):
  name = None

  def combine(self, other):
    """
    Combine two Types to produce the least flexible Type that covers both
    Types. The following rules are used:
    - AnyType & XType = AnyType
    - NullType & XType = XType
    - XType & XType = XType
    - IntegerType & NumberType = NumberType
    - XType & YType = AnyType
    """
    typ = type(self)
    if not isinstance(other, typ):
      typ = AnyType
    return typ()

  def to_dict(self):
    return {'type': self.name}

  @classmethod
  def parse(cls, value):
    """
    Parses a JSON compatible Python value and returns the Type object that
    matches it. For non-complex types like Strings and Booleans, we simply
    create an instance of the corresponding Type and return. This method
    needs overriding for more complex types like Arrays and Objects.
    """
    return cls()


class AnyType(Type):
  name = 'any'

  def combine(self, other):
    return AnyType()


class NullType(Type):
  name = 'null'

  def combine(self, other):
    return other


class IntegerType(Type):
  name = 'integer'

  def combine(self, other):
    if isinstance(other, NumberType):
      return NumberType()
    return super(IntegerType, self).combine(other)
  

class NumberType(Type):
  name = 'number'

  def combine(self, other):
    if isinstance(other, IntegerType):
      return NumberType()
    return super(NumberType, self).combine(other)


class BooleanType(Type):
  name = 'boolean'


class StringType(Type):
  name = 'string'


class ArrayType(Type):
  name = 'array'

  def __init__(self, items=None):
    self.items = items

  def combine(self, other):
    if not isinstance(other, ArrayType):
      return AnyType()
    return ArrayType(items=self.items.combine(other.items))

  def to_dict(self):
    _dict = super(ArrayType, self).to_dict()
    _dict['items'] = self.items.to_dict()
    return _dict

  @classmethod
  def parse(cls, value):
    item_schema = NullType()
    for item in value:
      item_schema = item_schema.combine(get_schema_type(item))
    return ArrayType(items=item_schema)


class ObjectType(Type):
  name = 'object'

  def __init__(self, properties=None, required=None):
    self.properties = properties or {}
    if required is None:
      required = list(properties)
    self.required = required

  def combine(self, other):
    if not isinstance(other, ObjectType):
      return AnyType()
    properties = {}
    required_properties = set(self.properties) & set(other.properties)
    for prop, typ in self.properties.iteritems():
      properties[prop] = typ
    for prop, typ in other.properties.iteritems():
      if prop in properties:
        properties[prop] = properties[prop].combine(typ)
      else:
        properties[prop] = typ
    return ObjectType(properties=properties,
                      required=sorted(required_properties))

  def to_dict(self):
    _dict = super(ObjectType, self).to_dict()
    _dict['properties'] = {}
    for prop, typ in self.properties.iteritems():
      _dict['properties'][prop] = typ.to_dict()
    _dict['required'] = self.required
    return _dict

  @classmethod
  def parse(cls, value):
    properties = {}
    for key, v in value.iteritems():
      properties[key] = get_schema_type(v)
    return ObjectType(properties=properties)


SCHEMA_TYPES = {
  types.NoneType: NullType,
  types.UnicodeType: StringType,
  types.StringType: StringType,
  types.IntType: IntegerType,
  types.LongType: IntegerType,
  types.FloatType: NumberType,
  types.BooleanType: BooleanType,
  types.ListType: ArrayType,
  types.DictType: ObjectType
  }


def get_schema_type(value):
  schema_type = SCHEMA_TYPES.get(type(value))
  if schema_type is None:
    raise ValueError
  return schema_type.parse(value)
