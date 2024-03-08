from os.path import join, abspath, dirname
from six import string_types
import yaml


def get_extends_app_dir():
    package_dir = abspath(dirname(__file__))
    return join(package_dir, 'extend_apps')


def load_app_yml_file(name):
    return yaml_load(join(get_extends_app_dir(), '%s.yml' % name))


def yaml_write(data, path):
    if not isinstance(path, string_types):
        raise AssertionError("Path must be string, got: %s" % path)

    with open(path, 'w') as f:
        content = yaml_dumps(data)
        # print(content)
        f.write(content)


def yaml_dumps(data):
    return yaml.safe_dump(data, default_flow_style=False)


def yaml_load(path):
    with open(path) as f:
        ret = yaml.safe_load(f)
        # pprint(ret)

        return ret


def yaml_loads(content):
    return yaml.safe_load(content)


def yaml_load_stream(stream):
    return yaml.safe_load(stream)