#!/usr/bin/env python3
"""Pulls config values out of the full blueprint config and writes it to
a javascript file to be read by angular.

    Usage:
        generate_angular_config.py INPUT_FILE OUTPUT_FILE

    Arguments:
        INPUT_FILE      the path to the blueprint config json file
        OUTPUT_FILE     where to write config variables js definition angular needs
"""
import json

from docopt import docopt


def main(args):
    """main entry point"""
    with open(args['INPUT_FILE']) as f:
        data = json.load(f)

    varlines = []
    for k, v in data['angularConfig'].items():
        varlines.append('{}: "{}"'.format(k, v))
    output = """angular.module('blueprint').constant('configuration', {{
    {}
}});""".format(",\n    ".join(varlines))
    with open(args['OUTPUT_FILE'], 'w') as f:
        f.write(output)


if __name__ == '__main__':
    main(docopt(__doc__))
