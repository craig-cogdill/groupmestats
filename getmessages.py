#!/usr/bin/env python3

import argparse

def sanitize_args():
    arg_parser = argparse.ArgumentParser(description="Get a list of messages from a GroupMe chat as CSV")
    arg_parser.add_argument("-t",
                            "--token",
                            dest="token",
                            metavar="TOKEN",
                            help="GroupMe API access token")
    return arg_parser.parse_args()

def main(input_args):
    print("Hello world")

if __name__ == '__main__':
    input_args = sanitize_args()
    main(input_args)
