"""
Command line wrapper for the Schema Builder tool.
"""

import argparse
import sys

from dbt import flags
from dbt.flags import get_flag_dict

from .builder import SchemaBuilderTask

PROFILES_DIR = get_flag_dict().get('PROFILES_DIR')


def parse_args(args):
    """
    Parse command line args.
    """
    p = argparse.ArgumentParser(
        prog="schema_builder: ",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="Select one of these sub-commands and you can find more help from there.",
    )

    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument(
        '--project-dir',
        default=None,
        type=str,
        help='''
            Which directory to look in for the dbt_project.yml file.
            Default is the current working directory and its parents.
            '''
    )

    base_subparser.add_argument(
        "--profiles-dir",
        default=PROFILES_DIR,
        type=str,
        help="""Which directory to look in for the profiles.yml file. Default = {}""".format(
            PROFILES_DIR
        ),
    )
    base_subparser.add_argument(
        "--profile",
        required=False,
        type=str,
        help="""Which profile to load. Overrides setting in dbt_project.yml.""",
    )
    base_subparser.add_argument(
        "--target",
        default=None,
        type=str,
        help="Which target to load for the given profile",
    )
    base_subparser.add_argument(
        "--threads",
        default=None,
        type=int,
        help="Number of threads for dbt to run.",
    )

    group = base_subparser.add_mutually_exclusive_group()

    group.add_argument(
        "--nopii",
        required=False,
        action='store_true',
        help="Whether or not to supress PII models and sources",
        default=False,
    )
    group.add_argument(
        "--piionly",
        required=False,
        action='store_true',
        help="Only create PII models and sources",
        default=False,
    )
    subs = p.add_subparsers(title="Available sub-commands", dest="command")

    build_sub = subs.add_parser(
        "build",
        parents=[base_subparser],
        help="Creates or updates schema.yml files from database catalog",
    )
    build_sub.set_defaults(cls=SchemaBuilderTask, which="build", defer=None, state=None, defer_state=None)

    build_sub.add_argument(
        "--destination-project",
        required=True,
        help="Required. Specify the project that will use the generated sources, relative to the source project.",
    )

    if not args:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)
    flags.set_from_args(parsed, {})
    return parsed


def handle(args):
    """
    Execute the given command. Currently only "build" exists, but it can be expanded here.
    """
    parsed = parse_args(args)

    if parsed.command == "build":
        task = SchemaBuilderTask(parsed)
        task.run(no_pii=parsed.nopii, pii_only=parsed.piionly)


def main(args=None):
    """
    Do main things.
    """
    if args is None:
        args = sys.argv[1:]

    handle(args)


if __name__ == "__main__":
    main()
