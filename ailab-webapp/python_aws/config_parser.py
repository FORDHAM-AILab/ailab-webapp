import yaml
import os
import re
import pathlib
from functools import lru_cache
import typing
import boto3
from botocore.exceptions import ClientError
import json
from typing import Optional, Union

# Default config path
default_config_path = pathlib.Path(__file__).parent.resolve().joinpath('config.yml')

default_secret_keys = None
sub_modules = ['redis', 'mongo']
db_required_fields = ["host", "username", "password", "database", "port"]

@lru_cache
def secret_client():
    return boto3.client(service_name='secretsmanager')


@lru_cache
def get_secret(secret_name: str) -> dict:
    """
    Pull secret df from AWS secret manager, return a dictionary of secrets
    """
    sc = secret_client()
    try:
        value = sc.get_secret_value(SecretId=secret_name)
        return json.loads(value['SecretString'])
    except ClientError as e:
        raise e


def get_secret_keys() -> Optional[dict]:
    return default_secret_keys


def set_secret_keys(secret_keys: Optional[dict]) -> bool:
    global default_secret_keys
    default_secret_keys = secret_keys
    return True


def get_value(key: str) -> Optional[Union[str, int]]:
    """
    Get value for an variable.  Use variable value in secret manager if it exists, otherwise, check the environment
    variables. Default to None
    """
    secret_keys = get_secret_keys()

    if secret_keys:
        for i in sub_modules:
            if i in key:
                secret_id = secret_keys[i]
                if secret_id:
                    secret_value = get_secret(secret_id)
                    for sk, sv in secret_value.items():
                        if sk in key:
                            return sv

    value = os.environ.get(key, "")
    return value


@lru_cache()
def parse_config(path: typing.Optional[str] = default_config_path,
                 data: typing.Optional[str] = None, tag: str = '!ENV') -> typing.Dict[str, dict]:
    """
    Load a yaml configuration file and resolve any environment variables
    The environment variables must have !ENV before them and be in this format
    to be parsed: ${VAR_NAME}.
    E.g.:
    database:
        host: !ENV ${HOST}
        port: !ENV ${PORT}
    app:
        log_path: !ENV '/var/${LOG_PATH}'
        something_else: !ENV '${AWESOME_ENV_VAR}/var/${A_SECOND_AWESOME_VAR}'
    :param str path: the path to the yaml file
    :param str data: the yaml df itself as a stream
    :param str tag: the tag to look for
    :return: the dict configuration
    :rtype: dict[str, T]
    """

    # pattern for global vars: look for ${word}
    pattern = re.compile('.*?\${(\w+)}.*?')
    loader = yaml.SafeLoader

    # the tag will be used to mark where to start searching for the pattern
    # e.g. somekey: !ENV somestring${MYENVVAR}blah blah blah
    loader.add_implicit_resolver(tag, pattern, None)

    def constructor_env_variables(loader, node):
        """
        Extracts the environment variable from the node's value
        :param yaml.Loader loader: the yaml loader
        :param node: the current node in the yaml
        :return: the parsed string that contains the value of the environment
        variable
        """
        value = loader.construct_scalar(node)
        match = pattern.findall(value)  # to find all env variables in line
        if match:
            full_value = value
            for g in match:
                replace_value = get_value(g)
                if full_value == f'${{{g}}}':
                    full_value = replace_value
                else:
                    full_value = full_value.replace(
                        f'${{{g}}}', replace_value
                    )
            return full_value
        return value

    loader.add_constructor(tag, constructor_env_variables)

    if path:
        with open(path) as conf_data:
            cfg = yaml.load(conf_data, Loader=loader)
    elif data:
        cfg = yaml.load(data, Loader=loader)
    else:
        raise ValueError('Either a path or df should be defined as input')

    # This is a second pass at parsing and now with secrets
    if 'secrets' in cfg:
        set_secret_keys(cfg['secrets'])
        if path:
            with open(path) as conf_data:
                cfg = yaml.load(conf_data, Loader=loader)
        elif data:
            cfg = yaml.load(data, Loader=loader)
        # Construct configures for various database
        for db in cfg['db_sys'].keys():
            # Get secrets from AWS secrets manager
            sec_key = get_secret_keys()
            all_db_secrets = get_secret(sec_key[db])
            cfg[f'{db}_config'] = {db_key: all_db_secrets[db_key] for db_key in db_required_fields}
    else:
        raise ValueError('Could\'t find secret setting in the config')

    return cfg
