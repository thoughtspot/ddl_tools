import locale

from pytql.tql import RemoteTQL

from dt.model import Database, Worksheet, Table
from dt.util import eprint, ConfigFile

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
    print(f"reviewing relationships vs. foreign keys for {database.database_name} database")
    # TODO implement test.
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
    TODO optimize the search to avoid checking the same thing.
    """
    print(f"reviewing circular relationships for {database.database_name} database")

    issues = []

    # dictionary of all tables and a set of all tables they are related to.
    table_relationships = {}
    for table in database:
        table_relationships[table.table_name] = set(table.get_all_related_tables())

    # Run through each table and add the related tables from the ones already related.
    # If the new set contains the original table, then there is a circular reference.  If the list doesn't get
    # any bigger, then the search is complete and go to the next table.
    for table_name in table_relationships:
        relationships = table_relationships[table_name]
        done = False
        while not done:
            number_relationships_before = len(relationships)
            new_related_tables = set(relationships)
            for related_table in relationships:
                for rt in table_relationships[related_table]:
                    new_related_tables.add(rt)
            relationships = new_related_tables
            if table_name in relationships:  # circled back around to this one.
                issues.append(f"{table_name} has circular relationship back to itself.")
                done = True
            elif len(relationships) == number_relationships_before:  # no more tables added.
                done = True

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


MAX_REFERENCE_CHAIN = 3


def review_long_chain_relationships(database, config_file):
    """
    Reviewing the database for the relationships that span multiple tables in between.
    :param database: The database to review.
    :type database: Database
    :param config_file: Configuration file for the max OK length.  Key: max_reference_chain, default=3
    :type config_file: ConfigFile
    :return: A list of recommendations.
    :rtype: list of str
    """
    print(f"reviewing long chain (> 2) relationships for {database.database_name} database")
    issues = []

    table_relationship_map = get_relationships(database=database)
    table_paths = []
    for table_name in table_relationship_map.keys():
        added_paths = get_related_paths(table_relationship_map=table_relationship_map, table_path=[table_name])
        if added_paths:
            table_paths.extend([p for p in added_paths])

    max_reference_chain = config_file.get("max_reference_chain", default=MAX_REFERENCE_CHAIN)
    for path in table_paths:
        if len(path) - 1 > max_reference_chain:
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
    print(f"reviewing M:M for {database.database_name} database")
    issues = []

    table_relationship_map = get_relationships(database=database)
    # TODO implement tests.

    return issues


MAX_ROWS_PER_SHARD = 10000000
MIN_ROWS_PER_SHARD = 5000000
MIN_SKEW_RATIO = 0.01


def review_sharding(database, rtql, config_file):
    """
    Reviews the sharding on tables for the database.
    Will report on oversharding, undersharding, high skew.
    Does not check for co-sharding issues.
    :param database: The database to review.  Only the name is needed.
    :type database: Database
    :param rtql: The remote TQL object.
    :type rtql: RemoteTQL
    :param config_file: The configuration file with settings to use.
    :type config_file: ConfigFile
    :return: A list of recommendations.
    :rtype: list of str
    """
    print(f"reviewing sharding for {database.database_name}")

    max_rows_per_shard = config_file.get("max_rows_per_shard", default=MAX_ROWS_PER_SHARD)
    min_rows_per_shard = config_file.get("min_rows_per_shard", default=MIN_ROWS_PER_SHARD)
    min_skew_ratio = config_file.get("min_skew_ratio", default=MIN_SKEW_RATIO)

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
                if total_row_count > max_rows_per_shard:
                    issues.append(f"{database_name}.{schema_name}.{table_name} is not sharded and has more than "
                                  f"{max_rows_per_shard:,} rows total")

            else:  # sharded tables
                # over sharded
                if (total_row_count / total_shards) < min_rows_per_shard:
                    issues.append(f"{database_name}.{schema_name}.{table_name} is sharded and has less than "
                                  f"{min_rows_per_shard:,} rows per shard")
                elif (total_row_count / total_shards) > max_rows_per_shard:
                    issues.append(f"{database_name}.{schema_name}.{table_name} is sharded and has more than "
                                  f"{max_rows_per_shard:,} rows per shard")

                skew_ratio = row_count_skew/(total_row_count/total_shards) if total_row_count > 0 else 0
                if skew_ratio > min_skew_ratio:
                    issues.append(f"{database_name}.{schema_name}.{table_name} has a high skew of {skew_ratio}")

    return issues


def review_pks(database):
    """
    Check that all tables have a PK.  It's not necessarily wrong, but gives a warning that they don't exist.
    :param database: The database to review.  Only the name is needed.
    :type database: Database
    :return: A list of recommendations.
    :rtype: list of str
    """
    print(f"reviewing primary keys for {database.database_name} database")
    issues = []

    for table in database:
        if not table.primary_key:
            issues.append(f"No primary key on table {database.database_name}.{table.schema_name}.{table.table_name}")

    return issues


def review_worksheet_joins(database, rtql, worksheet):
    """
    Reviews the join types in a worksheet.
    Constraints:
      * currently only supports one database scenarios and one table name in the database.
      * doesn't support fqn or alias values
      * currently only supports joins (foreign keys) and not generic joins
    :param database: The database that the worksheet uses.
    :type database: Database
    :param rtql: The remote TQL to use for database queries.
    :type rtql: RemoteTQL
    :param worksheet: The worksheet with the joins.
    :type worksheet: Worksheet
    :return: A list of recommendations.
    :rtype: list of str
    """
    # TODO add support for more table types and multiple tables in the same DB with the same name.
    print(f"reviewing worksheet joins for worksheet {worksheet.name} using {database.database_name} database")

    issues = []

    # TODO implement the test.
    # get the queries - including columns needed for multi-column joins.
    # execute the queries.
    # compare results to join type.

    for join in worksheet.get_joins():
        source_table_name = join.source
        source_table = database.get_table(table_name=source_table_name)
        source_table_type = worksheet.get_table(table_name=source_table_name).table_type
        if source_table_type != "table":
            eprint(f"Table {source_table_name} of type {source_table_type} not supported.  Only 'table' type supported.")
            continue
        source_db = database.database_name
        source_schema = database.get_table(table_name=source_table_name).schema_name

        destination_table_name = join.destination
        # destination_table = database.get_table(table_name=destination_table_name)
        destination_table_type = worksheet.get_table(table_name=destination_table_name).table_type
        if destination_table_type != "table":
            eprint(f"Table {destination_table_name} of type {destination_table_type} not supported. "
                   f"Only 'table' type supported.")
            continue
        destination_db = database.database_name
        destination_schema = database.get_table(table_name=destination_table_name).schema_name

        # get the join between the tables.
        join_name = join.name
        foreign_key = source_table.get_foreign_key(fk_name=join_name)
        if not foreign_key:
            eprint(f"Unable to access {foreign_key} for join {join_name}.  Only foreign keys supported.")
            continue
        join_on = ""
        for col_cnt in range(0, len(foreign_key.from_keys)):
            if col_cnt > 0:
                join_on += " AND "
            join_on += f'("{source_db}"."{source_schema}"."{source_table_name}"."{foreign_key.from_keys[col_cnt]}" = ' \
                       f'"{destination_db}"."{destination_schema}"."{destination_table_name}"."{foreign_key.to_keys[col_cnt]}")'

        source_fks = ",".join([f'"{source_db}"."{source_schema}"."{source_table_name}"."{k}"'
                               for k in foreign_key.from_keys])
        left_fk = foreign_key.from_keys[0]  # OK to just use one column.
        right_pk = foreign_key.to_keys[0]  # OK to just use one column.
        destination_pks = ",".join([f'"{destination_db}"."{destination_schema}"."{destination_table_name}"."{k}"'
                                   for k in foreign_key.to_keys])

        # If this query returns rows, then there are FK values in the left table that are not in the right table.
        query_source_table_name_has_missing_fks = \
            f'select ' \
            f'{source_fks}, ' \
            f'{destination_pks} ' \
            f'FROM ' \
            f'"{source_db}"."{source_schema}"."{source_table_name}" ' \
            f'full outer join ' \
            f'"{destination_db}"."{destination_schema}"."{destination_table_name}" ' \
            f'ON {join_on} ' \
            f'WHERE "{destination_table_name}"."{left_fk}" = NULL;'
        print(query_source_table_name_has_missing_fks)

        source_missing_fk_results = rtql.execute_tql_query(query_source_table_name_has_missing_fks)
        should_be_left_outer_join = source_missing_fk_results.nbr_rows() > 0

        # If this query returns rows, then there are FK values in the left table that are not in the right table.
        query_destination_table_name_has_missing_pks = \
            f'select ' \
            f'{source_fks}, ' \
            f'{destination_pks} ' \
            f'FROM ' \
            f'"{source_db}"."{source_schema}"."{source_table_name}" ' \
            f'full outer join ' \
            f'"{destination_db}"."{destination_schema}"."{destination_table_name}" ' \
            f'ON {join_on} ' \
            f'WHERE "{source_table_name}"."{right_pk}" = NULL;'
        print(query_destination_table_name_has_missing_pks)

        destination_missing_pk_results = rtql.execute_tql_query(query_destination_table_name_has_missing_pks)
        should_be_right_outer_join = destination_missing_pk_results.nbr_rows() > 0
        should_be_join = "INNER"
        if should_be_right_outer_join and should_be_left_outer_join:
            should_be_join = "OUTER"
        elif should_be_right_outer_join:  # can't be both
            should_be_join = "RIGHT_OUTER"
        elif should_be_left_outer_join:
            should_be_join = "LEFT_OUTER"

        actual_join = join.type  # type: [RIGHT_OUTER | LEFT_OUTER | INNER | OUTER]
        if actual_join != should_be_join:
            issues.append(f"Join {join_name} is type {actual_join}, but should probably be {should_be_join}.")

        pass

    return issues
