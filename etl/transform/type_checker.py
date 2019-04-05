from bson.json_util import ObjectId
import datetime
import re
from etl.extract import collection
from etl.monitor import logger


def get_type_pg(item):
    """
    Returns the item with its PG type.
    In case of list it detects if it consist of primitive types.
    If yes, it will return the corresponding type.
    Otherwise it will be a json array.
    Also, if a string is found, the ' will be replaced with '' so PG parser
    can know it's an apostrophe.
    """
    pg_type = None
    item_type = type(item)

    types = {
        "bool": 'boolean',
        "int": 'double precision',
        "float": 'double precision',
        "str": 'text',
        "datetime.datetime": 'timestamp',
        "ObjectId": 'text',
        "dict": 'boolean',
        "list": 'jsonb',
    }
    
    if str(item_type) in types.keys():
        pg_type = types[str(item_type)]
    elif item_type is type(None):
        item = 'null'

    else:
        pg_type = 'jsonb'

    return item, pg_type


def rename(name_old, type_orig, type_new):
    name_new = None
    if type_equal(type_orig, type_new) is True:
        return name_new
    elif type_new == 'text':
        if type_orig not in ['character', 'text']:
            name_new = "%s_t" % name_old
    elif type_new == 'float':
        name_new = "%s_f" % name_old
    elif type_new == 'boolean':
        name_new = "%s_b" % name_old
    elif type_new == 'integer':
        name_new = "%s_i" % name_old
    return name_new


def type_equal(old, new):
    equal_char = ('char' in old and 'char' in new)
    equal_array = (old == 'array' and new == 'jsonb')
    equal_float = (old == 'double precision' and new == 'float')
    if equal_char or equal_array or equal_float:
        return True
    return False


def is_convertable(type_old, type_new):
    """
    Returns True if type old can be converted to type new

    convertables: list of tuples
                : contains convertable types (type_old, type_new)
    """
    convertables = [
        ('boolean', 'double precision'),
        ('boolean', 'jsonb'),
        ('boolean', 'timestamp'),
        ('boolean', 'text'),

        ('double precision', 'boolean'),  # TODO: test again
        # ('double precision', 'jsonb'), # cannot
        ('double precision', 'timestamp'),
        ('double precision', 'text'),

        ('jsonb', 'text'),  # TODO: test again
        ('jsonb', 'double precision'),
        ('jsonb', 'timestamp'),
        ('jsonb', 'text'),

        ('timestamp', 'boolean'),
        ('timestamp', 'double precision'),
        ('timestamp', 'jsonb'),
        ('timestamp', 'text'),  # works (tested)

        ('text', 'jsonb'),  # not possible
        ('text', 'boolean'),
        ('text', 'double precision'),
        ('text', 'timestamp'),  # works (tested)
    ]
    if (type_old.lower(), type_new.lower()) in convertables:
        return True
    return False


def snake_case(word):
    return re.sub('([A-Z]+)', r'_\1', word).lower().strip("_")
