schema = {
    'database': {
        'type': 'dict',
        'schema': {
            'type': {'type': 'string', 'allowed': ['sqlite', 'postgresql', 'mysql']},
            'name': {'type': 'string', 'required': True},
            'host': {'type': 'string', 'required': False, 'default': 'localhost'},
            'port': {'type': 'integer', 'required': False},
            'user': {'type': 'string', 'required': False},
            'password': {'type': 'string', 'required': False},
        }
    }
}

def validate_config(config):
    from cerberus import Validator
    v = Validator(schema)
    if not v.validate(config):
        raise ValueError(f"Invalid configuration: {v.errors}")
    
    db_type = config['database']['type']
    if db_type in ['postgresql', 'mysql']:
        required_fields = ['host', 'port', 'user', 'password']
        defaults = {
            'postgresql': {'port': 5432, 'name': 'VarNoiseDB'},
            'mysql': {'port': 3306, 'name': 'VarNoiseDB'}
        }
        for field in required_fields:
            if field not in config['database']:
                config['database'][field] = defaults[db_type].get(field, 'localhost')
        if 'name' not in config['database']:
            config['database']['name'] = defaults[db_type]['name']
    
    return config
