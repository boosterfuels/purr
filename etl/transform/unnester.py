from bson import ObjectId
from bson.json_util import default
import json
import datetime
from etl.monitor import logger


def cast(column_type, value):
    '''
    Casts value based on required column type in collections.yml.
    When serializing ObjectId and datetime, a string is returned instead of objects with $oid and $date keys.
    Example:
      { _id: { $oid: "56ac26cc05b37b4e1a4d2c22" } }
      { _id: "56ac26cc05b37b4e1a4d2c22" }

    Parameters:
    -----------
    column_type : string
                  type of column
    value       : anything
                  value to cast 
    '''

    # if value is already None, it will be NULL, no need for casting
    if value is None:
        return value
        
    new_value = None

    column_type = column_type.lower()

    if column_type == 'text':
        new_value = str(value)

    elif column_type == 'timestamp':
        new_value = value

    elif column_type == 'double precision':
        new_value = None
        try:
            new_value = float(str(value))
        except ValueError as e:
            new_value = 'undefined'

    elif column_type == 'jsonb':
        new_value = json.dumps(value, default=str)

    elif column_type == 'boolean':
        new_value = bool(value)

    return new_value
