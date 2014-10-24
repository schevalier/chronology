import json
import unittest

from src.json_schema import AnyType
from src.json_schema import ArrayType
from src.json_schema import BooleanType
from src.json_schema import IntegerType
from src.json_schema import NullType
from src.json_schema import NumberType
from src.json_schema import ObjectType
from src.json_schema import StringType
from src.json_schema import get_schema_type


class SchemaTest(unittest.TestCase):
  def test_get_schema_type(self):
    # Basic parsing.
    self.assertEqual(type(get_schema_type(False)), BooleanType)
    self.assertEqual(type(get_schema_type(1)), IntegerType)
    self.assertEqual(type(get_schema_type(1.6)), NumberType)
    self.assertEqual(type(get_schema_type(None)), NullType)
    self.assertEqual(type(get_schema_type('lolcat')), StringType)
    self.assertEqual(type(get_schema_type([1])), ArrayType)
    self.assertEqual(type(get_schema_type({'a': 'b'})), ObjectType)

    # Array parsing.
    schema = get_schema_type([1, 2, 3])
    self.assertEqual(type(schema), ArrayType)
    self.assertEqual(type(schema.items), IntegerType)
    schema = get_schema_type([1, False, 'hello'])
    self.assertEqual(type(schema), ArrayType)
    self.assertEqual(type(schema.items), AnyType)

    # Object parsing.
    schema = get_schema_type({'lol': 'cat', 'int': 10, 'bool': True,
                              'nested': {'hello': 'world'}})
    self.assertEqual(type(schema), ObjectType)
    self.assertEqual(len(schema.properties), 4)
    self.assertEqual(set(schema.properties), {'lol', 'int', 'bool', 'nested'})
    self.assertEqual(len(schema.required), 4)
    self.assertEqual(set(schema.required), {'lol', 'int', 'bool', 'nested'})
    self.assertEqual(type(schema.properties['lol']), StringType)
    self.assertEqual(type(schema.properties['int']), IntegerType)
    self.assertEqual(type(schema.properties['bool']), BooleanType)
    nested = schema.properties['nested']
    self.assertEqual(type(nested), ObjectType)
    self.assertEqual(len(nested.properties), 1)
    self.assertEqual(set(nested.properties), {'hello'})
    self.assertEqual(len(nested.required), 1)
    self.assertEqual(set(nested.required), {'hello'})
    self.assertEqual(type(nested.properties['hello']), StringType)

  def test_combining(self):
    a = {
      'bool': True,
      'string': 'hello',
      'int': 1,
      'number': 2,
      'null': None,
      'any': False,
      'array_int': [1, 2],
      'array_any': [False],
      'object': {
        'lol': 'cat'
      },
      'not_required1': 1
    }
    b = {
      'bool': False,
      'string': 'world',
      'int': 23,
      'number': 2.56,
      'null': None,
      'any': 'yo',
      'array_int': [3, 4],
      'array_any': ['any'],
      'object': {
        'lol': 'cat',
        'not_required2': 'blah'
      },
      'null_not_required': None
    }
    merged = get_schema_type(a).combine(get_schema_type(b))
    expected = {
      'properties': {
        'any': {
          'type': 'any'
        },
        'array_any': {
          'items': {
            'type': 'any'
          },
          'type': 'array'
        },
        'array_int': {
          'items': {
            'type': 'integer'
          },
          'type': 'array'
        },
        'bool': {
          'type': 'boolean'
        },
        'int': {
          'type': 'integer'
        },
        'not_required1': {
          'type': 'integer'
        },
        'null': {
          'type': 'null'
        },
        'number': {
          'type': 'number'
        },
        'object': {
          'properties': {
            'lol': {
              'type': 'string'
            },
            'not_required2': {
              'type': 'string'
            }
          },
          'required': ['lol'],
          'type': 'object'
        },
        'string': {
          'type': 'string'
        },
        'null_not_required': {
          'type': 'null'
        },
      },
      'required': sorted(['bool', 'string', 'int', 'number', 'null', 'any',
                          'array_int', 'array_any', 'object']),
      'type': 'object'
    }
    self.assertEqual(json.dumps(merged.to_dict(), sort_keys=True),
                     json.dumps(expected, sort_keys=True))
