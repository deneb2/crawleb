"""Utility to read the configuration file."""
import yaml
import sys

def read_from_file(config_file, env="test"):
    cfg = None
    try:
        with open(config_file, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
    except IOError:
        # INFO: initialization error. Writing to stderr
        sys.stderr.write("Error: file not found: %s\n" %config_file)
        sys.exit(0)

        if cfg == None:
            sys.stderr.write("Error: config_file:'%s' is maybe empty\n" %config_file)
            sys.exit(0)

    setting = cfg.get(env)

    if not setting:
        sys.stderr.write("Error: configuration %s not found.\n" %(env,))
        sys.exit(0)
    return setting
