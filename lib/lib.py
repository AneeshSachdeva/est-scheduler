import yaml

def load_yaml(config_file_name):
    config = None
    with open('config/{}.yaml'.format(config_file_name), 'r') as stream:
        try:
            config = yaml.load(stream)
            return config
        except yaml.YAMLError as exc:
            print(exc)
            raise
