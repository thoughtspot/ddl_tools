import locale

from dt.model import Database
from pytql.tql import RemoteTQL

locale.setlocale(locale.LC_ALL, '')

"""
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

Contains all of the functions for reviewing database models.

Functions will be named "review_xxx" and accept one or more of the following named parameters.
  * database - Database object representing the database being tested.
  * worksheet - Worksheet object representing the worksheet being tested.  Might be None.
  * rtql - Remote TQL connect to do database queries.  Might be None.
  
The functions should return a list of recommendations from the test as a list of strings.
"""


def get_relationships(database):
    """
    Creates a relationship mapping of tables to related tables (without keys).
    :param database: The database to create the mapping for.
    :type database: Database
    :return: A dictionary that has a table to related tables list.
    :rtype: dict of list of str
    """
    table_relationship_map = {}
    for table in database.tables.values():
        related_tables = set()
        for foreign_key in table.foreign_keys.values():
            related_tables.add(foreign_key.to_table)
        table_relationship_map[table.table_name] = list(related_tables)

        for relationship in table.relationships.values():
            related_tables.add(relationship.to_table)
        table_relationship_map[table.table_name] = list(related_tables)

    return table_relationship_map


def review_relationships(database):
    """
    Reviewing the database for the relationships that can be replaced with foreign keys.
    :param database: The database to review.
    :type database: Database
    :return: A list of recommendations.
    :rtype: list of str
    """
    print(f"reviewing relationships vs. foreign keys for {database.database_name}")
    return []


def relates_to_previous(table_relationship_map, table_from_name, table_to_name):
    """
    Sees if the first table has a relationship in the relationships.  Calls recursively until there are no tables or
    a relationship is found.
    :param table_relationship_map: Map of tables to direct relationships.
    :type table_relationship_map: dict of list of str
    :param table_from_name: The table to start with for relationships.
    :type table_from_name: str
    :param table_to_name: The table to look for a relationships.
    :type table_to_name: str
    :return: True if there is a relationship.
    """

    # first look at the immediate tables to see if there are relationships.
    for table_name in table_relationship_map[table_from_name]:
        if table_name == table_to_name:
            return True

    # haven't found, so try the children for relationships.
    for table_name in table_relationship_map[table_from_name]:
        found = relates_to_previous(table_relationship_map=table_relationship_map,
                                    table_from_name=table_name, table_to_name=table_to_name)
        if found:
            return True

    return False


def review_circular_relationships(database):
    """
    Reviewing the database for circular or self referencing relationships.
    :param database: The database to review.
    :type database: Database
    :return: A list of recommendations.
    :rtype: list of str
    """
    print(f"reviewing circular relationships for {database.database_name}")

    issues = []

    table_relationship_map = get_relationships(database=database)
    for table in table_relationship_map.keys():
        found = relates_to_previous(table_relationship_map=table_relationship_map,
                                    table_from_name=table, table_to_name=table)

        if found:
            issues.append(f"{table} has circular relationship back to itself.")

    return issues


def get_related_paths(table_relationship_map, table_path):
    """
    Gets a list of lists that show all paths from the original list.  Each call creates new lists to represent the
    paths from the table based on the new table.
    :param table_relationship_map: Map of tables to direct relationships.
    :type table_relationship_map: dict of list of str
    :param table_path: The current path being added to.
    :type table_path: list of str
    :return: List of lists to represent each path.  First is the original table, last is the final table.
    :rtype: list of list of str
    """

    paths = []
    # get the last tables and start adding from there.
    last_table = table_path[-1]
    for related_table_name in table_relationship_map[last_table]:
        if related_table_name not in table_path:  # avoid circular references to infinity.
            new_path = table_path + [related_table_name]
            paths.append(new_path)
            added_paths = get_related_paths(table_relationship_map=table_relationship_map, table_path=new_path)
            if added_paths:
                paths.extend([p for p in added_paths])

    return paths


MAX_OK_PATH_LENGTH = 3


def review_long_chain_relationships(database):
    """
    Reviewing the database for the relationships that span multiple tables in between.
    :param database: The database to review.
    :type database: Database
    :return: A list of recommendations.
    :rtype: list of str
    """
    print(f"reviewing long chain (> 2) relationships for {database.database_name}")
    issues = []

    table_relationship_map = get_relationships(database=database)
    table_paths = []
    for table_name in table_relationship_map.keys():
        added_paths = get_related_paths(table_relationship_map=table_relationship_map, table_path=[table_name])
        if added_paths:
            table_paths.extend([p for p in added_paths])

    for path in table_paths:
        if len(path) - 1 > MAX_OK_PATH_LENGTH:  # need to acount for first table, so path is one less.
            issues.append(f"Long path ({len(path)}):  {path}.")

    return issues


def review_many_to_many_relationships(database, rtql):
    """
    Reviews the database to determine if there are M:M relationships based on the data.
    :param database: The database object.
    :type database: Database
    :param rtql: A remote connection to the database for queries.
    :type rtql: RemoteTQL
    :return: A list of recommendations.
    :rtype: list of str
    """
    print(f"reviewing M:M for {database.database_name}")
    issues = []

    table_relationship_map = get_relationships(database=database)

    return issues


MAX_ROWS_PER_SHARD = 10000000
MIN_ROWS_PER_SHARD = 5000000
MIN_SKEW_RATIO = 0.01


def review_sharding(database, rtql):
    """
    Reviews the sharding on tables for the database.
    Will report on oversharding, undersharding, high skew.  Does not check for co-sharding issues.
    :param database: The database to review.  Only the name is needed.
    :type database: Database
    :param rtql: The remote TQL object.
    :type rtql: RemoteTQL
    :return:
    """
    print(f"reviewing M:M for {database.database_name}")
    issues = []
    results = rtql.execute_tql_query("show statistics for server;")
    database_name = database.database_name
    for row in results:
        if row.get_column("Database Name") == database_name:
            schema_name = row.get_column("Schema Name")
            table_name = row.get_column("Table Name")
            total_row_count = int(row.get_column("Total Row Count"))
            total_shards = int(row.get_column("Total Shards"))
            row_count_skew = int(row.get_column("Row Count Skew"))

            if total_shards == 1:  # unsharded tables.
                # large, unsharded table
                if total_row_count > MAX_ROWS_PER_SHARD:
                    issues.append(f"{database_name}.{schema_name}.{table_name} is not sharded and has more than "
                                  f"{MAX_ROWS_PER_SHARD:,} rows total")

            else:  # sharded tables
                # over sharded
                if (total_row_count / total_shards) < MIN_ROWS_PER_SHARD:
                    issues.append(f"{database_name}.{schema_name}.{table_name} is sharded and has less than "
                                  f"{MIN_ROWS_PER_SHARD:,} rows per shard")
                elif (total_row_count / total_shards) > MAX_ROWS_PER_SHARD:
                    issues.append(f"{database_name}.{schema_name}.{table_name} is sharded and has more than "
                                  f"{MAX_ROWS_PER_SHARD:,} rows per shard")

                skew_ratio = row_count_skew/(total_row_count/total_shards) if total_row_count > 0 else 0
                if skew_ratio > MIN_SKEW_RATIO:
                    issues.append(f"{database_name}.{schema_name}.{table_name} has a high skew of {skew_ratio}")

    return issues
