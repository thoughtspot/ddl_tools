"""
Contains all of the classes for working with a ThoughtSpot data model.  

Copyright 2017 ThoughtSpot

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
"""

from collections import OrderedDict
from copy import copy, deepcopy

# -------------------------------------------------------------------------------------------------------------------


class DatamodelConstants:
    """
    Constants for working with data models.
    """
    DEFAULT_SCHEMA = "falcon_default_schema"


# -------------------------------------------------------------------------------------------------------------------


class Column:
    """
    Represents a single column in a table.
    """

    # Valid column types in ThoughtSpot.
    VALID_TYPES = [
        "VARCHAR",
        "DOUBLE",
        "FLOAT",
        "BOOL",
        "INT",
        "BIGINT",
        "DATE",
        "DATETIME",
        "TIMESTAMP",
        "TIME",
        "UNKNOWN",
    ]

    def __init__(self, column_name, column_type):
        """
        Creates a new column with the given name and type.
        :param column_name:  The name of the column.
        :type column_name: str
        :param column_type:  The type of the column.  This will be checked to verify it's a valid type.
        :type column_type: str
        """
        assert column_name is not None
        assert column_type is not None

        self.column_name = column_name
        if column_type not in Column.VALID_TYPES and not column_type.startswith(
            "VARCHAR"
        ):
            raise ValueError(f"{column_type} is not a valid column type for column with name {column_name}.")

        self.column_type = column_type


# -------------------------------------------------------------------------------------------------------------------


class ForeignKey:
    """
    Represents a foreign key relationship with another table.
    """

    def __init__(self, from_table, from_keys, to_table, to_keys, name=None):
        """
        Creates a foreign key relationship to another table.  Number of to_keys and from_keys must match.
        :param from_table:  Name of the table this key is coming from.
        :param from_keys:  Name of the key column or list of columns.
        :param to_table:  Table this foreign key links to.
        :param to_keys:  Column name or list of column names to the other table.  
        :param name: Name of the foreign key.  If not provided, one will be created.
        """
        assert from_table is not None
        assert from_keys is not None
        assert to_table is not None
        assert to_keys is not None

        self.from_table = from_table
        self.from_keys = list()
        if isinstance(from_keys, list):
            self.from_keys.extend(from_keys)
        else:
            self.from_keys.append(from_keys)

        self.to_table = to_table
        self.to_keys = list()
        if isinstance(to_keys, list):
            self.to_keys.extend(to_keys)
        else:
            self.to_keys.append(to_keys)

        if not len(self.from_keys) == len(self.to_keys):
            raise ValueError("%s has different length keys:  %s:%s -- %s:%s" %
                             (name, self.from_table, self.from_keys, self.to_table, self.to_keys))

        if name is None:
            self.name = "FK_%s_to_%s" % (from_table, to_table)
        else:
            self.name = name

    def __eq__(self, other):
        """
        Compares contents to see if the two are the same or not.  Equal if all content is equal.
        :param other:  The other ForeignKey to compare to.
        :type other: ForeignKey
        :return: True if they are the same.
        """
        return self.name == other.name and \
            self.from_table == other.from_table and sorted(self.from_keys) == sorted(self.to_keys) and \
            self.to_table == other.to_table and sorted(self.to_keys) == sorted(self.to_keys)

# -------------------------------------------------------------------------------------------------------------------


class GenericRelationship:
    """
    Represents a generic relationship between two different tables.
    """

    def __init__(self, from_table, to_table, conditions, name=None):
        """
        Creates a relationship between tables.
        :param from_table:  Name of the table this relationship is coming from.
        :param to_table:  Table this relationship links to.
        :param name: Name of the relationship.  If not provided, one will be created.  Note that there can be multiple
        relationships between tables, so using the default for both will cause invalid models.
        :param conditions:  Complete condition, properly formatted for TQL.
        :type conditions: str
        """
        assert from_table is not None
        assert to_table is not None
        assert conditions is not None

        if name is None:
            self.name = "REL_%s_to_%s" % (from_table, to_table)
        else:
            self.name = name

        self.from_table = from_table
        self.to_table = to_table

        self.conditions = conditions

    def __eq__(self, other):
        """
        Compares contents to see if the two are the same or not.  Equal if all content is equal.
        :param other:  The other GenericRelationship to compare to.
        :type other: GenericRelationship
        :return: True if they are the same.
        """
        return self.name == other.name and \
            self.from_table == other.from_table and \
            self.to_table == other.to_table and \
            self.conditions == other.conditions

# -------------------------------------------------------------------------------------------------------------------


class ShardKey:
    """
    Represents a shard key on a table.
    """

    def __init__(self, shard_keys, number_shards):
        """
        Creates a has key with the given columns and number of shards.
        :param shard_keys: One or more columns to use for the shard key.
        :param number_shards: Number of shards to use.
        :type number_shards: int
        """

        assert shard_keys is not None
        assert number_shards is not None

        self.shard_keys = list()
        if isinstance(shard_keys, list):
            self.shard_keys.extend(shard_keys)
        else:
            self.shard_keys.append(shard_keys)

        self.number_shards = number_shards


# -------------------------------------------------------------------------------------------------------------------


class Table:
    """
    Table for holding columns and relationships.
    """

    def __init__(
        self,
        table_name,
        schema_name=DatamodelConstants.DEFAULT_SCHEMA,
        primary_key=None,
        shard_key=None
    ):
        """
        Creates a new table.
        :param table_name: Name of the table.
        :type table_name: str
        :param schema_name: Name of the schema the table belongs to.  Defaults to Database.DEFAULT_SCHEMA.
        :type schema_name: str
        :param primary_key: Name of a column or list of columns that represent the primary key.
        :param shard_key: Shard key to use for sharded tables.
        :type shard_key: ShardKey
        """
        assert table_name is not None
        assert schema_name is not None

        self.table_name = table_name
        self.schema_name = schema_name

        self.primary_key = list()
        if primary_key is not None:
            if isinstance(primary_key, list):
                self.primary_key.extend(primary_key)
            else:
                self.primary_key.append(primary_key)
        if shard_key:  # make sure the class is only passing in the shard key and not just a list of columns.
            assert isinstance(shard_key, ShardKey)
        self.shard_key = shard_key

        self.columns = OrderedDict()  # Preserve order of columns in tables, since this matters in ThoughtSpot.
        self.foreign_keys = OrderedDict()  # Foreign key relationships.
        self.relationships = OrderedDict()  # Relationships with other tables.

    def add_column(self, column):
        """
        Adds a column to a table.
        :param column:  The column object to add.
        ":type column:  Column
        """
        self.columns[column.column_name] = column

    def add_columns(self, columns):
        """
        Adds a list of columns to the table.
        :param columns: List of columns to add.
        :type columns: list
        """
        for column in columns:
            self.add_column(column)

    def drop_column(self, column_name):
        """
        Drops the column with the given name from the table.
        :param column_name:  Then name of the column to drop.
        :type column_name: str
        :return:  The column that was removed or None if the column didn't exist.
        :rtype: Column
        """
        return self.columns.pop(column_name, None)

    def has_column(self, column_name):
        """
        Returns true if the column wih the given name is in the table.
        :param column_name:  The name of the column to check for.
        :type column_name: str
        :return:  True if the column is in the table, False otherwise.
        """
        return column_name in self.columns.keys()

    def get_column(self, column_name):
        """
        Returns the column with the given name without removing it.  This is the actual column and not a copy, so 
        be careful of changing it.
        :param column_name: The name of the column to return.
        :type column_name: str
        :return: The column with the given name or none.
        :rtype: Column
        """
        return self.columns.get(column_name, None)

    def get_column_names(self):
        """
        Returns a list of column names in this table.
        :return: list of str
        """
        column_names = []
        for column_name in self.columns.keys():
            column_names.append(column_name)

        return column_names

    def number_columns(self):
        """
        Retrurns the number of columns in the table.
        :return: The number of columns in the table.
        :rtype: int
        """
        return len(self.columns)

    def __iter__(self):
        """
        Allows the table to be iterated over the columns.
        :return: An iteratable object for iterating on the columns.
        """
        return iter(self.columns.values())

    def set_primary_key(self, primary_key):
        """
        Sets the primary key.  This will overwrite any previous primary key.
        :param primary_key: Column or list of columns.
        """
        assert primary_key is not None  # doesn't make sense to call with no value.
        self.primary_key = list()
        if isinstance(primary_key, str):
            self.primary_key.append(primary_key)
        elif isinstance(primary_key, list):
            self.primary_key.extend(primary_key)
        else:
            raise ValueError(
                "Primary keys must be a string or list instead of %s."
                % type(primary_key)
            )

    def add_foreign_key(
        self,
        foreign_key=None,
        from_keys=None,
        to_table=None,
        to_keys=None,
        name=None,
    ):
        """
        Adds a foreign key to the table.  This can either be a created ForeignKey object, or the details of the key, 
        which will be created.
        :param foreign_key: The ForeignKey that was already created.  Other parameters will be ignored.
        :type foreign_key: ForeignKey
        :param from_keys: Name of key or list of keys.
        :param to_table: Name of table to link to.
        :param to_keys: Name of key or list of keys to link to.
        :param name: Optional name of the table.  One will be created if not provided.
        """
        if foreign_key is not None:
            if isinstance(foreign_key, ForeignKey):
                self.foreign_keys[foreign_key.name] = foreign_key
            else:
                raise ValueError(
                    "The foreign key must be of type ForeignKey, but got %s"
                    % type(foreign_key)
                )

        else:
            fk = ForeignKey(
                from_table=self.table_name,
                from_keys=from_keys,
                to_table=to_table,
                to_keys=to_keys,
                name=name,
            )
            self.foreign_keys[fk.name] = fk

    def get_foreign_key(self, fk_name):
        """
        Returns the foreign key with the given name or none.
        :param fk_name: Name of the foreign key.
        :type fk_name: str
        :return: Foreign key with the given name or None if it doesn't exist.
        ":rtype: ForeignKey
        """
        return self.foreign_keys.get(fk_name, None)

    def foreign_keys_iter(self):
        """
        Returns an iterator over the foreign keys.
        :return: Iterator over the foreign keys.
        :rtype: iter
        """
        return iter(self.foreign_keys.values())

    # def __init__(self, from_table, from_keys, to_table, to_keys, name=None, conditions=None):

    def add_relationship(
        self, relationship=None, to_table=None, name=None, conditions=None
    ):
        """
        Adds a foreign key to the table.  This can either be a created ForeignKey object, or the details of the key, 
        which will be created.
        :param relationship: The GenericRelationship that was already created.  Other parameters will be ignored.
        :type relationship: GenericRelationship
        :param to_table: Name of table to link to.
        :param name: Optional name of the table.  One will be created if not provided.
        :param conditions: Optional conditions for the relationship.  
        """
        if relationship is not None:
            if isinstance(relationship, GenericRelationship):
                self.relationships[relationship.name] = relationship
            else:
                raise ValueError(
                    "The relationship must be of type GenericRelationship, but got %s"
                    % type(relationship)
                )

        else:
            rel = GenericRelationship(
                from_table=self.table_name,
                to_table=to_table,
                name=name,
                conditions=conditions,
            )
            self.relationships[rel.name] = rel

    def get_relationship(self, rel_name):
        """
        Returns the foreign key with the given name or none.
        :param rel_name: Name of the foreign key.
        :type rel_name: str
        :return: Relationship with the given name or None if it doesn't exist.
        ":rtype: Relationship
        """
        return self.relationships.get(rel_name, None)

    def relationships_iter(self):
        """
        Returns an iterator over the relationships.
        :return: Iterator over the relationships.
        :rtype: iter
        """
        return iter(self.relationships.values())

    def get_shard_key_columns(self):
        """
        Returns the columns used for partiioning or None if not partitioned.
        :return: Returns the columns used for partiioning or None if not partitioned.
        :rtype: list of str | None
        """
        if self.shard_key:
            return self.shard_key.shard_keys
        return None

    def get_number_shards(self):
        """
        If the table has a shard key, then return the number of shards.  Useful for directly getting.
        :return: The number of shards or None if the table isn't partitioned.
        :rtype: int | None
        """
        if self.shard_key:
            return self.shard_key.number_shards
        return None

    def get_all_related_tables(self):
        """
        Returns a list with the names of all related tables, either foreign keys or generic relationships.
        :return: A list with the names of all related tables, either foreign keys or generic relationships.
        :rtype: list of str
        """
        all_related_tables = []
        all_related_tables.extend([fk.to_table for fk in self.foreign_keys.values()])
        all_related_tables.extend([rel.to_table for rel in self.relationships.values()])
        return all_related_tables





# -------------------------------------------------------------------------------------------------------------------


class ValidationResult:
    """
    Provides the results of validation.
    """

    INFO = "Information"
    WARNING = "Warning"
    ERROR = "Error"

    def __init__(self):
        """
        Creates a new validation result that is valid with no issues.
        """
        self.is_valid = True
        self.issues = []

    def add_issue(self, issue, level=ERROR):
        """
        Adds an issue to the validation results, automatically making it invalid.
        :param issue: The issue to add.
        :type issue: str
        :param level: The level of the error (INFO, WARNING, ERROR).
        :type level: str
        """
        self.is_valid = False
        self.issues.append((issue, level))

    def add_error(self, issue):
        """
        Adds a validation error.
        :param issue: The issue to add.
        :type issue: str
        """
        self.add_issue(issue, ValidationResult.ERROR)

    def add_warning(self, issue):
        """
        Adds a validation warning.
        :param issue: The issue to add.
        :type issue: str
        """
        self.add_issue(issue, ValidationResult.WARNING)

    def add_info(self, issue):
        """
        Adds a validation info.
        :param issue: The issue to add.
        :type issue: str
        """
        self.add_issue(issue, ValidationResult.INFO)

    def eprint_issues(self):
        """
        Prints the issues to standard error.
        """
        for issue in self.issues:
            eprint(issue[1] + ":  " + issue[0])


# -------------------------------------------------------------------------------------------------------------------


class Database:
    """
    Class that represents a database.  A database contains schemas and tables.
    Note that tables with the same name in different schemas are not currently supported.
    """
    # TODO Add support for tables with the same name in different schemas.

    def __init__(self, database_name):
        """
        Creates a new database with the given name.
        :param database_name: Name of the database.
        :type database_name: str
        """
        assert database_name is not None
        self.database_name = database_name
        self.tables = OrderedDict()
        self.schemas = {}

    def add_table(self, table):
        """
        Adds a table to the database.
        :param table: table to add to the database.
        ":type table: Table
        """
        self.tables[table.table_name] = table

        # increment so that the schema can be deleted.
        nbr_schema = self.schemas.get(table.schema_name, 0)
        nbr_schema += 1
        self.schemas[table.schema_name] = nbr_schema

    def get_table(self, table_name):
        """
        Returns the table with the given name.
        :param table_name: Name of the table to return.
        :return: Table with the given name or None if it's not in the database.
        :rtype: Table
        """
        return self.tables.get(table_name, None)

    def get_table_names(self):
        """
        Returns the names of the tables.
        :return:  The name of the tables.
        :rtype: list
        """
        return self.tables.keys()

    def number_tables(self):
        """
        Return the number of tables in the database.
        :return: The number of tables in the database.
        :rtype: int
        """
        return len(self.tables.keys())

    def __iter__(self):
        """
        Returns an iterator on the tables.
        :return: Iterator for the tables.
        """
        return iter(self.tables.values())

    def drop_table(self, table_name):
        """
        Drops a table from the database.
        :param table_name: The name of the table to drop.
        :type table_name: str
        :return: The table to drop or None if the table doesn't exist.
        :rtype: Table
        """
        table = self.tables.pop(table_name, None)
        if table is not None:
            schema_name = table.schema_name
            nbr_schema = self.schemas[schema_name]
            nbr_schema -= 1
            if nbr_schema == 0:
                self.schemas.pop(schema_name)
            else:
                self.schemas[schema_name] = nbr_schema

        return table

    def get_schema_names(self):
        """
        Returns a list of schema names.
        :return: The list of schemas in the database.
        :rtype: list
        """
        return self.schemas.keys()

    def validate(self):
        """
        Validates that the model does not contain any errors.
        :return: A validation object with is_valid set to True or False and list of issues if not valid.
        :rtype: ValidationResult
        """
        return DatabaseValidator(self).validate()

    def get_number_relationships_from_table(self, table_name):
        """
        Returns the number of foreign keys and generic relationships from a given table.
        :param table_name: The name of the table to get relationships for.
        :type table_name: str
        :return: The number of foreign keys and relationships from a given table.
        :rtype: int
        """
        table = self.get_table(table_name=table_name)
        if table:
            return len(table.foreign_keys.values()) + len(table.relationships.values())
        raise ValueError(f"Unkonwn table {table_name}")

    def get_number_relationships_to_table(self, table_name):
        """
        Returns the number of foreign keys to a given table.
        :param table_name: The name of the table to get relationships for.
        :type table_name: str
        :return: The number of foreign keys to a given table.
        :rtype: int
        """
        if not table_name in self.get_table_names():
            raise ValueError(f"Unkonwn table {table_name}")

        number_relationships = 0
        for table in self.tables.values():
            for fk in table.foreign_keys.values():
                if fk.to_table == table_name:
                    number_relationships += 1
            for rel in table.relationships.values():
                if rel.to_table == table_name:
                    number_relationships += 1

        return number_relationships

# -------------------------------------------------------------------------------------------------------------------


class DatabaseValidator:
    """
    Validates databases for consistency.  Any database that passes validation should load into
    ThoughtSpot with no errors.
    """

    def __init__(self, database):
        """
        Creates a new DatabaseValidator
        :param database:  A database to validate.
        :type database: Database
        """
        self.database = database
        self.validation_results = ValidationResult()

    def validate(self):
        """
        Validates the database returning a validation result.
        :return: A validation result object.
        :rtype: ValidationResult
        """

        for table in self.database:
            self._validate_column_types(table)
            self._validate_primary_key(table)
            self._validate_shard_keys(table)
            self._validate_foreign_keys(table)
            self._validate_relationships(table)

        return self.validation_results

    def _add_validation_issue(
        self, table, issue, level=ValidationResult.ERROR
    ):
        """
        Adds a formatted validation message.
        :param table: The table being validated.
        :type table: Table
        :param issue: The issue to add.
        :type issue: str
        """
        self.validation_results.add_issue(
            "database %s, table %s:  %s"
            % (self.database.database_name, table.table_name, issue),
            level=level,
        )

    def _validate_column_types(self, table):
        """
        Validates that the column types are all known.
        :param table: The table being validated.
        :type table: Table
        """
        for column in table.columns.values():
            if column.column_type == "UNKNOWN":
                self._add_validation_issue(
                    table=table,
                    issue="column %s is of type UNKNOWN." % column.column_name,
                    level=ValidationResult.WARNING,
                )

    def _validate_primary_key(self, table):
        """
        Validates that the primary key is a key in this table.
        :param table: The table being validated.
        :type table: Table
        """
        pks = table.primary_key
        for pk in pks:
            if table.get_column(pk) is None:
                self._add_validation_issue(
                    table=table,
                    issue="column %s in primary key does not exist in the the table."
                    % pk,
                )

    def _validate_shard_keys(self, table):
        """
        Validates the shard keys for a given table.
        :param table: The table to validate for.
        :type table: Table
        """
        # Shard keys must be part of the primary key.
        sks = table.shard_key
        pks = table.primary_key

        if sks is not None:
            for sk in sks.shard_keys:
                if table.get_column(sk) is None:
                    self._add_validation_issue(
                        table=table,
                        issue="column %s in shard key does not exist in the the table."
                        % sk,
                    )

                if pks != [] and sk not in pks:
                    self._add_validation_issue(
                        table=table,
                        issue="column %s in shard key not in primary key %s"
                        % (sk, pks),
                    )

    def _validate_foreign_keys(self, table):
        """
        Validates the foreign keys for a given table.
        :param table: The table to validate for.
        :type table: Table
        """
        for fk in table.foreign_keys_iter():

            to_table = self.database.get_table(fk.to_table)
            # make sure the other table exists in the database.
            if to_table is None:
                self._add_validation_issue(
                    table=table,
                    issue="table %s doesn't exist for foreign key %s"
                    % (fk.to_table, fk.name),
                )
            else:
                # The foreign keys need to match the primary key of the other table.

                # match number of columns in from and to
                if len(fk.from_keys) != len(fk.to_keys):
                    self._add_validation_issue(
                        table=table,
                        issue="FK %s doesn't have the matching column count from and to keys"
                        % fk.name,
                    )
                # verify to keys match number of columns in primary key of other table.
                if len(fk.to_keys) != len(to_table.primary_key):
                    self._add_validation_issue(
                        table=table,
                        issue="FK %s doesn't match number of columns in primary key %s"
                        % (fk.name, to_table.primary_key),
                    )
                # verify to keys match types of columns in primary key of other table.
                for col_cnt in range(0, len(fk.from_keys)):
                    from_name = fk.from_keys[col_cnt]
                    from_col = table.get_column(from_name)
                    to_name = fk.to_keys[col_cnt]
                    to_col = to_table.get_column(fk.to_keys[col_cnt])

                    if to_name not in to_table.primary_key:
                        self._add_validation_issue(
                            table=table,
                            issue="foreign key %s column %s isn't in primary key for %s"
                            % (fk.name, to_name, to_table.table_name),
                        )

                    missing_column = False
                    if from_col is None:
                        self._add_validation_issue(
                            table=table,
                            issue="foreign key %s missing from_column %s from table %s"
                            % (fk.name, from_name, table.table_name),
                        )
                        missing_column = True

                    if to_col is None:
                        self._add_validation_issue(
                            table=table,
                            issue="foreign key %s missing to_column %s from table %s"
                            % (fk.name, to_name, table.table_name),
                        )
                        missing_column = True

                    if not missing_column:
                        if (
                            not from_col.column_type.startswith("VARCHAR")
                        ) and (
                            from_col.column_type != to_col.column_type
                        ):
                            self._add_validation_issue(
                                table=table,
                                issue="foreign key %s column %s type %s doesn't match type %s for %s column %s"
                                % (
                                    fk.name,
                                    from_col.column_name,
                                    from_col.column_type,
                                    to_col.column_type,
                                    to_table.table_name,
                                    to_col.column_name,
                                ),
                            )

                        elif (
                            from_col.column_type.startswith("VARCHAR")
                            and not to_col.column_type.startswith("VARCHAR")
                            or to_col.column_type.startswith("VARCHAR")
                            and not from_col.column_type.startswith("VARCHAR")
                        ):
                            self._add_validation_issue(
                                table=table,
                                issue="foreign key %s column %s type %s doesn't match type %s for %s column %s"
                                % (
                                    fk.name,
                                    to_name,
                                    from_col.column_type,
                                    to_col.column_type,
                                    to_table.table_name,
                                    to_col.column_name,
                                ),
                            )

    def _validate_relationships(self, table):
        """
        Validates the relationships for a given table.
        :param table: The table to validate for.
        :type table: Table
        """
        for rel in table.relationships_iter():
            if rel.from_table != table.table_name:
                self._add_validation_issue(
                    table=table,
                    issue="from table %s doesn't exist in relationship %s."
                    % (table.table_name, rel.name),
                )

            to_table = self.database.get_table(rel.to_table)
            # make sure the other table exists in the database.
            if to_table is None:
                self._add_validation_issue(
                    table=table,
                    issue="to table %s doesn't exist in relationship %s."
                    % (to_table, rel.name),
                )

# -------------------------------------------------------------------------------------------------------------------


class WorksheetTable:
    """Represents a table in a worksheet."""

    def __init__(self, table_name, table_type="table"):
        """
        Creates a worksheet table of the given type.
        :param table_name: The name of the table.
        :type table_name: str
        :param table_type: The type of table, either "table", "alias", or "fqn" (aka GUID).
        :type table_type: str
        """
        assert table_name
        assert table_type in ["table", "alias", "fqn"]

        self.table_name = table_name
        self.table_type = table_type


class WorksheetJoin:
    """Represents a join between tables in a worksheet."""

    def __init__(self, j_name, j_source, j_destination, j_type, j_is_one_to_one):
        """
        Creates a new worksheet join with the given values.
        :param j_name: The name of the join.
        :type j_name: str
        :param j_source: The source table of the join.
        :type j_source: str
        :param j_destination: The destination table of the join.
        :type j_destination: str
        :param j_type: The type of join (INNER, OUTER, etc.)
        :param j_type: str
        :param j_is_one_to_one: True if a 1:1 join.
        :type j_is_one_to_one: bool
        """
        self.name = j_name
        self.source = j_source
        self.destination = j_destination
        self.type = j_type
        self.is_one_to_one = j_is_one_to_one


class WorksheetTablePath:
    """Represents a table path between two tables."""

    # TODO need to test this out with more complex joins.  Might be overly simple.
    def __init__(self, path_id, table, join_paths):
        """
        Creates a new table path for the worksheet.
        :param path_id: The id for the path.
        :type path_id: str
        :param table:  The table for the start of the path.
        :type table: str
        :param join_paths: The path(s) used for the join.
        :type join_paths: list of str
        :returns: None
        """
        self.path_id = path_id
        self.table = table

        self.join_paths = []
        if join_paths:
            self.join_paths.extend(join_paths)


class WorksheetFormula:
    """Represents a formula in a worksheet."""

    def __init__(self, name, expression, formula_id=None):
        """
        Creates a new worksheet expression.
        :param name:  The name of the expression.
        :type name: str
        :param expression:  The actual expression.
        :type expression: str
        :param formula_id: An optional ID for the formula.
        :type formula_id: str
        """
        assert name
        assert expression

        self.name = name
        self.expression = expression
        self.formula_id = formula_id


class WorksheetColumn:
    """Represents a column in a worksheet."""

    def __init__(self, name, column_id, column_properties, is_formula=False):
        """
        Creates a new worksheet column.
        :param name:  The name of the column.
        :type name: str
        :param column_id: The full ID for the column.  This maps to the table and column.
        :type column_id: str
        :param column_properties:  Properties on the column.
        :type column_properties: dict[str, str]
        :param is_formula: True if this is a formula column.
        :type is_formula: bool
        """
        assert name
        assert column_properties is not None and isinstance(column_properties, dict)

        self.name = name
        self.column_id = column_id
        self.column_properties = copy(column_properties)
        self.is_formula = is_formula

    def get_path(self):
        """
        Returns the path to the column.  Only valid for columns and not formulas.
        :return: The path to the column.
        :rtype: [str | None]
        """
        if not self.is_formula:
            return self.column_id.split("::")[0]
        return None

    def get_source(self):
        """
        Returns the source of the column.  Only valid for columns and not formulas.
        :return: The path to the column.
        :rtype: [str | None]
        """
        if not self.is_formula:
            return self.column_id.split("::")[1]
        return None

    def get_property(self, property_name):
        """
        Returns the property of the given name or None if the column doesn't have that property.
        See Worksheet YAML specification in the ThoughtSpot documentation.
        :param property_name: The name of the property to get.
        :type property_name: str
        :return: The property value or None
        :rtype: [str | None]
        """
        return self.column_properties.get(property_name, None)


class Worksheet:
    """Represents a worksheet."""

    def __init__(self, name, description, properties):
        """
        Creates a worksheet with the given name.
        :param name: The name of the worksheet.
        :type name: str
        :param description: The description of the worksheet.
        :type description: str
        :param properties: The worksheet properties.
        :type properties: dict
        """
        assert name

        self.name = name
        self.description = description if description else ""
        self.properties = dict(properties) if properties else dict()

        self._tables = []
        self._joins = []
        self._table_paths = []
        self._formulas = []
        self._columns = []

    def add_table(self, table):
        """
        Adds a table to the worksheet.
        :param table: The name of a table.
        :type table: WorksheetTable
        :return: None
        """
        assert table
        self._tables.append(deepcopy(table))

    def get_tables(self):
        """
        Returns the tables for this worksheet.
        :return: The list of tables or an empty list.
        :rtype: list of WorksheetTable
        """
        return self._tables

    def get_table(self, table_name):
        """
        Returns the table with the given name.
        :param table_name: The name of the table to return.
        :type table_name: str
        :return: The worksheet table with the given name.
        :rtype: WorksheetTable | None
        """
        assert table_name
        for t in self._tables:
            if t.table_name == table_name:
                return t
        return None

    def add_join(self, join):
        """
        Adds a join to the worksheet.
        :param join: The name of a join.
        :type join: WorksheetJoin
        :return: None
        """
        assert join
        self._joins.append(deepcopy(join))

    def get_joins(self):
        """
        Returns the joins for this worksheet.
        :return: The list of joins or an empty list.
        :rtype: list of WorksheetJoin
        """
        return self._joins

    def add_table_path(self, table_path):
        """
        Adds a table path to the worksheet.
        :param table_path:
        :type table_path: WorksheetTablePath
        :return: None
        """
        assert table_path
        self._table_paths.append(deepcopy(table_path))

    def add_formula(self, formula):
        """
        Adds a formula to the worksheet.
        :param formula: The name of a formula.
        :type formula: WorksheetFormula
        :return: None
        """
        assert formula
        self._formulas.append(deepcopy(formula))

    def add_column(self, column):
        """
        Adds a column to the worksheet.
        :param column: The name of a column.
        :type column: WorksheetColumn
        :return: None
        """
        assert column
        self._columns.append(deepcopy(column))
