#!/usr/bin/env python

import sys
import argparse

from evaluation.evaluation import evaluate_text
from process.kb import load_gpickle


def show_result(result):
    print("The confidence is: {}".format(result[0]))
    print("The reputation is: {}".format(result[1]))


def evaluate(args):
    text = (args.f.read()
            if args.f else
            sys.stdin.read()
            )

    G = load_gpickle(args.g)
    show_result(evaluate_text(G, text, alpha=args.a))


def main(arguments):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-g', help="Graph in binary format.", required=True,
                        type=argparse.FileType('rb'))
    parser.add_argument('-f', help="# of pararel process",
                        type=argparse.FileType('r'))
    parser.add_argument('-a', help="Alpha value", type=float, default=.5)

    args = parser.parse_args(arguments)

    evaluate(args)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
