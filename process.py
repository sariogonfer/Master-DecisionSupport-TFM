#!/usr/bin/env python

import os
import sys
import argparse

from process.kb import update_kb, save_gpickle
from process.load import load_docs_from_dir, load_docs_from_es
from process.process import process_docs_from_iterable


def process(args):
    in_ = (load_docs_from_es(args.s, args.e)
           if args.e else
           load_docs_from_dir(args.s, args.d)
           )
    G = process_docs_from_iterable(in_, update_kb, n=args.n)
    save_gpickle(G, args.outfile)


def main(arguments):

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-s', help="# of docs to process", type=int,
                        required=True)
    parser.add_argument('-n', help="# of pararel process", type=int,
                        default=32)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '-d', help="Directory that contein the docs to process.")
    group.add_argument(
        '-e', help="Endpoint of the ES server where docs are storaged")
    parser.add_argument('-o', '--outfile', help="Output file", required=True,
                        type=argparse.FileType('wb'))

    args = parser.parse_args(arguments)

    process(args)

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
