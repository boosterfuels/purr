import json


def cast(column_type, value):
    '''
    Casts value based on required column type in collections.yml.
    When serializing ObjectId and datetime, a string is returned
    instead of objects with $oid and $date keys.
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
    new_value = None

    if value is None:
        return new_value

    column_type = column_type.lower()

    if column_type == 'text':
        new_value = str(value)

    elif column_type == 'timestamp':
        new_value = value

    elif column_type == 'double precision':
        try:
            new_value = float(str(value))
        except ValueError:
            new_value = 'undefined'

    elif column_type == 'jsonb':
        new_value = json.dumps(value, default=str)

    elif column_type == 'boolean':
        new_value = bool(value)

    return new_value
