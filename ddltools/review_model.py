"""
Converts from non-TS DDL to TS DDL.  $ convert_ddl.py --help for more details.

Copyright 2019 ThoughtSpot

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions
of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

NOTE:  There are many things that could be more efficient.
The following assumptions are made about the DDL being read:
  CREATE TABLE occur together on a single line, not split across lines.
  CREATE TABLE statements will not occur inside of a comment block.
  Delimiters, such as commas, will not be part of the table or column name.
  Comment characters, such as #, --, or /* */ will not be part of a column name.
  CREATE TABLE will have (....) with no embedded, unbalanced parentheses.
"""
import argparse
import logging
import os
import sys

from dt.util import eprint
from dt.io import DDLParser, YAMLWorksheetReader
from dt.review.review import DataModelReviewer
from pytql.tql import RemoteTQL

VERSION = "1.0"


def main():
    """Main function for the script."""
    parser = get_parser()
    args = parser.parse_args()

    if not valid_args(args):
        parser.print_help()

    else:
        print(args)

        sys.setrecursionlimit(10000)  # expanding from the default of 1000.  Might cause memory errors.

        database = None
        worksheet = None
        rtql = None

        if args.version:
            version_path = os.path.dirname(os.path.abspath(__file__))
            print(f"convert_ddl (v{VERSION}):  {version_path}/convert_ddl")
            exit(0)  # just exit if printing the version -- similar behavior to help.

        if args.debug:
            logging.basicConfig(level=logging.DEBUG)

        reviewer = DataModelReviewer()

        if args.show_tests:
            descriptions = reviewer.get_test_descriptions()
            print(f"Found {len(descriptions)} tests.")
            for test in descriptions.keys():
                print("")
                print(f"{test}:")
                for desc in descriptions[test]:
                    print(f"\t{desc}")
            print("")

        # create the database.
        if args.ts_ip:
            database = read_from_ts(args)
        elif args.database_file:
            parser = DDLParser(database_name=args.database)  # these tests ignore the schema name.
            database = parser.parse_ddl(filename=args.database_file)
        else:  # only continue if there is a database.
            exit(0)

        # read the worksheet.
        if args.worksheet_file:
            worksheet = YAMLWorksheetReader.read_from_file(args.worksheet_file)

        # create an RTQL object
        if args.ts_ip:
            rtql = RemoteTQL(hostname=args.ts_ip, username=args.username, password=args.password)

        reviewer = DataModelReviewer()
        results = reviewer.review_model(database=database, worksheet=worksheet, rtql=rtql)

        for test in results.keys():
            issues = results[test]
            print(f"{test}:")
            for issue in issues:
                print(f"\t{issue}")


def get_parser():
    """
    Parses the arguments from the command line.
    :return: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version", help="print the version and path to this script.",
        action="store_true"
    )
    parser.add_argument(
        "--show_tests", action = "store_true",
        help="list the tests and their descriptions"
    )
    parser.add_argument(
        "--database_file", help="will attempt to load database DDL from the file"
    )
    parser.add_argument(
        "--worksheet_file", help="worksheet description as YAML"
    )
    parser.add_argument("--ts_ip", help="IP or URL for ThoughtSpot cluster for DB schema and data queries")
    parser.add_argument("--username", default="admin",
                        help="command line username (e.g. admin) to use for authentication")
    parser.add_argument("--password", default="th0ughtSp0t", help="command line password to use for authentication")
    parser.add_argument(
        "-d", "--database", help="name of ThoughtSpot database"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging."
    )
    parser.add_argument(
        "--config_file",
        help="File with configuration values to use for the test."
    )

    return parser


def valid_args(args):
    """
    Checks to see if the arguments make sense.
    :param args: The command line arguments.
    :return: True if valid, false otherwise.
    """
    is_valid = True

    # allow descriptions to be shown and no tests run.
    if not args.show_tests:

        if not args.database:
            eprint("A database name must be provided.")
            is_valid = False

        if not args.database_file and not args.ts_ip:
            eprint("Either a database file or ThoughtSpot IP must be provided.")
            is_valid = False

    if args.database_file and args.ts_ip:
        eprint("Only database_file or ts_ip can be provided.")
        is_valid = False

    if args.worksheet_file:
        if not os.path.exists(args.worksheet_file):
            eprint(f"Worksheet file {args.worksheet_file} doesn't exist.")
            is_valid = False

    if args.config_file:
        if not os.path.exists(args.config_file):
            eprint(f"Configuration file {args.config_file} doesn't exist.")
            is_valid = False

    return is_valid


def read_from_ts(args):
    """
    Reads the database (from args) from TQL remotely.
    :param args: The argument list.  Must have the host, database and possibly user/password.
    :return: A database that was read.
    :rtype: Database
    """
    rtql = RemoteTQL(hostname=args.ts_ip, username=args.username, password=args.password)
    out = rtql.run_tql_command(f"script database {args.database};")

    # The parser expects a file, so create a temp file, parse, then delete.
    filename = f"{args.database}.tmp"
    with open(filename, "w") as outfile:
        for line in out:
            outfile.write(line + "\n")

    parser = DDLParser(database_name=args.database)
    database = parser.parse_ddl(filename=filename)
    os.remove(filename)

    return database


if __name__ == "__main__":
    main()
