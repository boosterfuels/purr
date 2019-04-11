from bson.json_util import ObjectId
import datetime
import re


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

    if item_type is bool:
        pg_type = 'boolean'

    elif item_type is int:
        pg_type = 'double precision'

    elif item_type is float:
        pg_type = 'double precision'

    elif item_type is str:
        pg_type = 'text'

    elif item_type is datetime.datetime:
        pg_type = 'timestamp'

    elif item_type is ObjectId:
        pg_type = 'text'

    elif item_type is dict:
        pg_type = 'jsonb'

    elif item_type is list:
        pg_type = 'jsonb'

    elif isinstance(item_type, type(None)):
        item = 'null'
    else:
        pg_type = 'jsonb'

    return item, pg_type


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
