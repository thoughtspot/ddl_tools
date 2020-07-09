# DDL TOOLS

DDL tools is a set of Python libraries and applications designed to make creating and working with ThoughtSpot DDL 
easier. 
 
## Setup

DDL Tools is installed with the same process as other TS Python tools.

You can install using `pip install --upgrade git+https://github.com/thoughtspot/ddl_tools`

See the general [documentation](https://github.com/thoughtspot/community-tools/tree/master/python_tools) on setting 
up your environment and installing using `pip`.

## Running the pre-built tools

All of the pre-built tools are run using the general format: 

`python -m ddltools.<tool-name>`

Note there is no `.py` at the end and you *must* use `python -m`.  So for example to run `convert_ddl` and see the 
options, you would enter `python -m ddl_tools.convert_ddl --help`  Try it now and verify your environment is all set.

The DDL tools currently consist of two scripts:
1. `convert_ddl`, converts between different formats including DDL for most major databases, TQL, and Excel.
2. `ddldiff` a tool to show the differences between two different schemas.  Note that this tool is currently beta.

## convert_ddl

~~~
usage: python -m ddltools.convert_ddl [-h] [--version] [--empty] [--from_ddl FROM_DDL]
                                      [--to_tql TO_TQL] [--from_ts FROM_TS] [--to_ts TO_TS]
                                      [--username USERNAME] [--password PASSWORD]
                                      [--from_excel FROM_EXCEL] [--to_excel TO_EXCEL]
                                      [-d DATABASE] [-s SCHEMA] [-c] [-l] [-u] [--camelcase]
                                      [-v] [--debug]

optional arguments:
  -h, --help            show this help message and exit
  --version             Print the version and path to this script.
  --empty               creates an empty modeling file.
  --from_ddl FROM_DDL   will attempt to convert DDL from the infile
  --to_tql TO_TQL       will convert to TQL and write to the outfile
  --from_ts FROM_TS     read from TS cluster at the given URL. May also need
                        username / password
  --to_ts TO_TS         (BETA) will convert to TQL and write to ThoughtSpot at
                        the given URL
  --username USERNAME   username to use for authentication
  --password PASSWORD   password to use for authentication
  --from_excel FROM_EXCEL
                        convert from the given Excel file
  --to_excel TO_EXCEL   will convert to Excel and write to the outfile.
  -d DATABASE, --database DATABASE
                        name of ThoughtSpot database
  -s SCHEMA, --schema SCHEMA
                        name of ThoughtSpot schema
  -c, --create_db       generate create database and schema statements
  -l, --lowercase       create table and column names in lowercase
  -u, --uppercase       create table and column names in uppercase
  --camelcase           converts table names and columns names with _ to camel
                        case, e.g. my_table becomes MyTable.
  -v, --validate        validate the database
  --debug               Prints details of parsing.
~~~

## ddldiff

Compares two DDL files and can generate alters to make the first match the second.

~~~
usage: python -m ddltools.ddl_diff 
       [-h] [--ddl1 DDL1] [--ddl2 DDL2] [--database DATABASE]
       [--schema SCHEMA] [--alter1] [--alter2] [--ignore_case]

optional arguments:
  -h, --help           show this help message and exit
  --ddl1 DDL1          DDL file containing the schema that would be changed.
  --ddl2 DDL2          DDL file containing the new schema.
  --database DATABASE  name of database for generating alter statements
  --schema SCHEMA      name of schema for generating alter statements
  --alter1             Generates drop, create, alter, etc. statements that
                       would be needed to make the first DDL align with the
                       second.
  --alter2             Generates drop, create, alter, etc. statements that
                       would be needed to make the second DDL align with the
                       first.
  --ignore_case        Causes case of names to be ignored
  ~~~

### Sample of common workflow to convert DDL

The standard workflow that we use with new DDL that we want to connvert uses the following steps:
* Convert from DDL to Excel
* Review and enhance the model in Excel
* Validate the model
* Convert from Excel to TQL DDL

After that, you can create the database tables in TQL.

### Sample commands

To convert from some other database DDL to Excel:
```
convert_ddly --from_ddl <somefile> --to_excel <somefile> --database <db-name>
```
This will result in a new excel file.  Note that the .xlsx extension is option on the excel file name.

To validate a model from Excel:
```
convert_ddl --from_excel <somefile>.xlsx --validate
```
You will either get errors or a message that the model is valid.

To convert from Excel to TQL DDL:
```bazaar
convert_ddl --from_excel <somefile>.xlsx --to_tql <somefile> 
```
You will get an output file that contains (hopefully) valid TQL syntax.

### Cleanup in Excel
convert_ddl does it's best to parse DDL from a wide variety of sources, but there are some feature gaps and 
occasional things you'll need to clean up.

WARNING:  Do not delete any of the existing columns.  Adding columns is OK since they will be ignored.
 
#### Columns tab
The first tab has all of the columns.  You should look for data types of "UNKNOWN".  These are data types
in the original that the parser couldn't decide how to translate.  We are constantly finding new types and adding.

You should also review the columns to see if you like the type chosen.  In particular, we default to BIGINT and DOUBLE
when it's not obvious.  But you may not need these wider types.  Also, some DATE types should be mapped to DATETIME 
instead of DATE.  Finally, don't worry about the actual size in the VARCHAR.  We just ignore this in ThoughtSpot.

#### Tables tab

Most of this tab is automatically generated, but you will want to update three columns:  primary key, shard key, and 
number of shards.  The last two are only needed if you want to shard the table.  The # rows column is optional and 
doesn't impact TQL generation, but it helps with determining shard needs.  

The PKs to and from columns tell the number of foreign key relationships for each table.  This can be convenient for
determining if the table have relationships defined.

#### Foreign keys tab

The foreign keys tab allows you to enter FK relationships between the tables.  The columns should be obvious.  
Note that ThoughtSpot requires a foreign key to reference a primary key and the number and type of columns
must match.  The validation step will check for these conditions.  

You can use any name you like.  A common practice is FK_fromtable_to_totable_by_columna.  You can use the following
formula in the first column to generate this formula.  Note that the quotes don't always paste corrrecty and this 
formula assumes it's in the second column.  Update as needed.

`="fk_"&D2&"_to_"&F2&"_by_"&SUBSTITUTE(SUBSTITUTE(E2," ",""),",","_and_")`

#### Relationships tab

The relationships tab lets you enter relationships between tables.  In general, you should try to use foreign keys, but 
sometimes relationships are more appropriate.  The condition must be a valid condition between the tables.  This 
condition is not validated by the validator other than it exists.

## Review Model

~~~
usage: python -m ddltools.review_model [-h] [--version] [--show_descriptions]
                                       [--database_file DATABASE_FILE]
                                       [--worksheet_file WORKSHEET_FILE] [--ts_ip TS_IP]
                                       [--username USERNAME] [--password PASSWORD]
                                       [-d DATABASE] [--debug]

Reviews a model to suggest improvements.  A database file is minimally required.  If a 
worksheet and/or connection is provided, additional tests will be run. 

optional arguments:
  -h, --help            show this help message and exit
  --version             print the version and path to this script.
  --show_tests          list the tests and their descriptions
  --database_file DATABASE_FILE
                        will attempt to load database DDL from the file
  --worksheet_file WORKSHEET_FILE
                        worksheet description as YAML
  --ts_ip TS_IP         IP or URL for ThoughtSpot cluster for DB schema and data
                        queries
  --username USERNAME   command line username (e.g. admin) to use for
                        authentication
  --password PASSWORD   command line password to use for authentication
  -d DATABASE, --database DATABASE
                        name of ThoughtSpot database
  --debug               Enable debug logging.

To see a list of tests run the following command.

python -m ddl_tools.review_model --show_tests

review_circular_relationships(database):
	Reviewing the database for circular or self referencing relationships.

review_long_chain_relationships(database, config_file):
	Reviewing the database for the relationships that span multiple tables in between.

review_many_to_many_relationships(database, rtql):
	Reviews the database to determine if there are M:M relationships based on the data.

review_pks(database):
	Check that all tables have a PK.  It's not necessarily wrong, but gives a warning that they don't exist.

review_relationships(database):
	Reviewing the database for the relationships that can be replaced with foreign keys.

review_sharding(database, rtql, config_file):
	Reviews the sharding on tables for the database.
	Will report on oversharding, undersharding, high skew.
	Does not check for co-sharding issues.

review_worksheet_joins(database, rtql, worksheet):
	Reviews the join types in a worksheet.
	Constraints:
	* currently only supports one database scenarios and one table name in the database.
	* doesn't support fqn or alias values
	* currently only supports joins (foreign keys) and not generic joins


~~~

### What is reviewed?

The number of tests is expected to increase over time, but the following are currently included:

~~~
review_circular_relationships(database):
	Reviewing the database for circular or self referencing relationships.

review_long_chain_relationships(database):
	Reviewing the database for the relationships that span multiple tables in between.

review_many_to_many_relationships(database, rtql):
	Reviews the database to determine if there are M:M relationships based on the data.

review_pks(database):
	Check that all tables have a PK.  It's not necessarily wrong, but gives a warning that they don't exist.

review_relationships(database):
	Reviewing the database for the relationships that can be replaced with foreign keys.

review_sharding(database, rtql):
	Reviews the sharding on tables for the database.
	Will report on oversharding, undersharding, high skew.
	Does not check for co-sharding issues.
~~~

### Sample commands

To review a database model HO_retail that is already present in TS:

`python -m ddltools.review_model --ts_url 10.10.10.001 --username admin --password password --database your_database`

To review a database model from a TQL DDL file:

`python -m ddltools.review_model --database_file your_database.tql --database your_database`

Note: --database name is mandatory in all the above cases because this name is used in the script to create a database 
object and print out the results. 



