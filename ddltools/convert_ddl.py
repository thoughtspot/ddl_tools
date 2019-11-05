#!/usr/bin/python
"""
Converts from non-TS DDL to TS DDL.  $ convert_ddl.py --help for more details.

Copyright 2017-2018 ThoughtSpot

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

from dt.model import Database, DatamodelConstants, eprint
from dt.io import DDLParser, TQLWriter, XLSWriter, XLSReader
from pytql.tql import RemoteTQL

VERSION="2.0"


def main():
    """Main function for the script."""
    args = parse_args()

    if valid_args(args):
        print(args)

        if args.version:
            version_path = os.path.dirname(os.path.abspath(__file__))
            print(f"convert_ddl (v{VERSION}):  {version_path}/convert_ddl")
            exit(0)  # just exit if printing the version -- similar behavior to help.

        if args.debug:
            logging.basicConfig(level=logging.DEBUG)

        if args.from_ddl:
            print("Reading DDL ...")
            database = read_ddl(args)
        elif args.from_excel:
            print("Reading Excel ...")
            database = read_excel(args)
        elif args.from_ts:
            print("Reading DDL from ThoughtSpot")
            database = read_from_ts(args)
        else:
            database = Database(database_name=args.database)

        if args.validate:
            print("Validating database")
            vr = database.validate()
            if not vr.is_valid:
                vr.eprint_issues()
            else:
                print("Database is valid.")

        if args.to_tql:
            print("Writing TQL ...")
            write_tql(args=args, database=database)

        if args.to_excel:
            print("Writing Excel ...")
            write_excel(args=args, database=database)

        if args.to_ts:
            print("Writing to ThoughtSpot ...")
            write_to_ts(args=args, database=database)

def parse_args():
    """Parses the arguments from the command line."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version", help="Print the version and path to this script.",
        action="store_true"
    )
    parser.add_argument(
        "--empty", help="creates an empty modeling file.",
        action="store_true"
    )
    parser.add_argument(
        "--from_ddl", help="will attempt to convert DDL from the infile"
    )
    parser.add_argument("--to_tql", help="will convert to TQL and write to the outfile")
    parser.add_argument("--from_ts", help="read from TS cluster at the given URL.  May also need username / password")
    parser.add_argument("--to_ts", help="(BETA) will convert to TQL and write to ThoughtSpot at the given URL")
    parser.add_argument("--username", default="admin", help="username to use for authentication")
    parser.add_argument("--password", default="th0ughtSp0t", help="password to use for authentication")
    parser.add_argument(
        "--from_excel", help="convert from the given Excel file"
    )
    parser.add_argument(
        "--to_excel", help="will convert to Excel and write to the outfile."
    )
    parser.add_argument(
        "-d", "--database", help="name of ThoughtSpot database"
    )
    parser.add_argument(
        "-s",
        "--schema",
        default="falcon_default_schema",
        help="name of ThoughtSpot schema",
    )
    parser.add_argument(
        "-c",
        "--create_db",
        action="store_true",
        help="generate create database and schema statements",
    )
    parser.add_argument(
        "-l",
        "--lowercase",
        action="store_true",
        help="create table and column names in lowercase",
    )
    parser.add_argument(
        "-u",
        "--uppercase",
        action="store_true",
        help="create table and column names in uppercase",
    )
    parser.add_argument(
        "--camelcase",
        action="store_true",
        help="converts table names and columns names with _ to camel case, "
        + "e.g. my_table becomes MyTable.",
    )
    parser.add_argument(
        "-v", "--validate", action="store_true", help="validate the database"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Prints details of parsing."
    )

    args = parser.parse_args()
    return args


def valid_args(args):
    """
    Checks to see if the arguments make sense.
    :param args: The command line arguments.
    :return: True if valid, false otherwise.
    """

    # make sure there is a to_ flag since data has to come from somewhere unless this is just creating blank Excel.
    if not args.empty and not args.version and not args.from_ddl \
            and not args.from_excel and not args.to_excel and not args.from_ts:
        eprint("--version, --empty, --from_ddl, --from_excel, or from_ts must be provided as arguments.")
        return False

    if (args.from_ddl or args.from_ts) and not args.database:
        eprint("--from_ddl and --from_ts require the --database parameter.")
        return False

    return True


def read_ddl(args):
    """
    Reads database DDL and returns a database model.
    :param args: The command line arguments.
    :returns: The database read from the DDL.
    :rtype: Database
    """
    parser = DDLParser(args.database, args.schema)
    return parser.parse_ddl(args.from_ddl)


def read_excel(args):
    """
    Reads the database description from XLS and returns a database model.
    Note that XLSReader can read multiple databases at a time, but convert 
    only supports one.  If there are more than one database, the first will be
    returned and an error message written.
    :param args: The command line arguments.
    :returns: The database read from Excel.
    :rtype: Database
    """

    reader = XLSReader()
    databases = reader.read_xls(filepath=args.from_excel)
    if len(databases) == 0:
        eprint("ERROR:  No databases read.")
        return None

    if len(databases) > 1:
        eprint(
            "WARNING:  multiple databases read.  Only using %s"
            % list(databases.values())[0].database_name
        )

    return list(databases.values())[0]


def read_from_ts(args):
    """
    Reads the database (from args) from TQL remotely.
    :param args: The argument list.  Must have the host, database and possibly user/password.
    :return: A database that was read.
    :rtype: Database
    """
    rtql = RemoteTQL(hostname=args.from_ts, username=args.username, password=args.password)
    out = rtql.run_tql_command(f"script database {args.database};")

    # The parser expects a file, so create a temp file, parse, then delete.
    filename = f"{args.database}.tmp"
    with open (filename, "w") as outfile:
        for line in out:
            outfile.write(line + "\n")

    # TODO - add parse from string option.
    parser = DDLParser(database_name=args.database)
    database = parser.parse_ddl(filename=filename)
    os.remove(filename)

    return database


def write_tql(args, database):
    """
    Writes the database to TQL to the output file.
    :param args: The command line arguments.
    :param database: The database to write.
    :type database: Database
    """
    writer = TQLWriter(
        args.uppercase, args.lowercase, args.camelcase, args.create_db
    )
    writer.write_tql(database, args.to_tql)


def write_excel(args, database):
    """
    Writes the database to Excel for analysis and expansion.
    :param args: The command line arguments.
    :param database: The database to write.
    :type database: Database
    :return None:
    """
    writer = XLSWriter()
    filename = args.to_excel
    if filename is None:
        filename = args.database + "_" + args.schema

    writer.write_database(database, filename)


def write_to_ts(args, database):
    """
    Writes the database directly to ThoughtSpot.
    :param args:  The command line arguments.
    :param database: The database to write.
    :type database: Database
    :return: None
    """
    rtql = RemoteTQL(hostname=args.to_ts, username=args.username, password=args.password)

    tempfile = f"{database.database_name}.tmp"

    writer = TQLWriter(
        args.uppercase, args.lowercase, args.camelcase, args.create_db
    )
    writer.write_tql(database, tempfile)

    # The parser expects a file, so create a temp file, parse, then delete.
    data = ""
    with open (tempfile, "r") as infile:
        for line in infile:
            data += line
    response = rtql.run_tql_command(data)
    os.remove(tempfile)
    print(response)


if __name__ == "__main__":
    main()
