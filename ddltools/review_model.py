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

from dt.model import eprint
from dt.io import DDLParser, YAMLWorksheetReader
from dt.review.review import DataModelReviewer
from pytql.tql import RemoteTQL

VERSION = "1.0"


def main():
    """Main function for the script."""
    args = parse_args()

    if valid_args(args):
        print(args)

        database = None
        worksheet = None
        rtql = None

        if args.version:
            version_path = os.path.dirname(os.path.abspath(__file__))
            print(f"convert_ddl (v{VERSION}):  {version_path}/convert_ddl")
            exit(0)  # just exit if printing the version -- similar behavior to help.

        if args.debug:
            logging.basicConfig(level=logging.DEBUG)

        # create the database.
        if args.database_file:
            parser = DDLParser(database_name=args.database)  # these tests ignore the schema name.
            database = parser.parse_ddl(filename=args.database_file)
        elif args.ts_url:
            database = read_from_ts(args)

        # read the worksheet.
        if args.worksheet_file:
            worksheet = YAMLWorksheetReader.read_from_file(args.worksheet_file)

        # create an RTQL object
        if args.ts_url:
            rtql = RemoteTQL(hostname=args.ts_url, username=args.username, password=args.password)

        reviewer = DataModelReviewer()
        results = reviewer.review_model(database=database, worksheet=worksheet, rtql=rtql)

        for test in results.keys():
            issues = results[test]
            print(f"{test}:")
            for issue in issues:
                print(f"\t{issue}")


def parse_args():
    """Parses the arguments from the command line."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version", help="Print the version and path to this script.",
        action="store_true"
    )
    parser.add_argument(
        "--from_tql", action="store_true", help="will attempt to load database DDL from the ThoughtSpot"
    )
    parser.add_argument(
        "--database_file", help="will attempt to load database DDL from the file"
    )
    parser.add_argument(
        "--worksheet_file", help="worksheet description as YAML"
    )
    parser.add_argument("--ts_url", help="URL for ThoughtSpot cluster for DB and data queries")
    parser.add_argument("--username", default="admin", help="username to use for authentication")
    parser.add_argument("--password", default="th0ughtSp0t", help="password to use for authentication")
    parser.add_argument(
        "-d", "--database", help="name of ThoughtSpot database"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging."
    )

    args = parser.parse_args()
    return args


def valid_args(args):
    """
    Checks to see if the arguments make sense.
    :param args: The command line arguments.
    :return: True if valid, false otherwise.
    """
    is_valid = True
    if not args.database:
        eprint("A database name must be provided.")
        is_valid = False

    if args.from_tql and not args.ts_url:
        eprint("Must provide a server to connect to when creating a database from TQL.")
        is_valid = False

    if args.worksheet_file:
        if not os.path.exists(args.worksheet_file):
            eprint(f"Worksheet file {args.worksheet_file} doesn't exist.")
            is_valid = False

    return is_valid


def read_from_ts(args):
    """
    Reads the database (from args) from TQL remotely.
    :param args: The argument list.  Must have the host, database and possibly user/password.
    :return: A database that was read.
    :rtype: Database
    """
    rtql = RemoteTQL(hostname=args.ts_url, username=args.username, password=args.password)
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
