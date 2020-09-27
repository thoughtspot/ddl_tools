import unittest

from dt.io import DDLParser, YAMLWorksheetReader
from dt.review.review import DataModelReviewer
from pytql.tql import RemoteTQL

# -------------------------------------------------------------------------------------------------------------------

TS_URL = "tstest"
TS_USER = "admin"
TS_PASSWORD = "Adm1n@th0ughtSp0t"


class TestModelReviewer(unittest.TestCase):
    """Tests the database reviewer."""

    def test_db_circular_relationship(self):
        """Tests a database for issues."""
        parser = DDLParser(database_name="test_db")
        database = parser.parse_ddl("circular.tql")

        reviewer = DataModelReviewer()
        issues = reviewer.review_model(database=database)

        self.assertEqual(6, len(issues["review_circular_relationships"]))

    def test_long_chain_relationships(self):
        """Tests if a relationship between two tables is longer than recommended."""
        parser = DDLParser(database_name="test_db")
        database = parser.parse_ddl("long_chain.tql")

        reviewer = DataModelReviewer()
        issues = reviewer.review_model(database=database)

        self.assertEqual(6, len(issues["review_long_chain_relationships"]))

    def test_sharding(self):
        """Tests the review of sharding.  This test assumes the sharding database has been loaded with data."""

        parser = DDLParser(database_name="review_test_sharding")
        database = parser.parse_ddl("test_sharding.tql")

        rtql = RemoteTQL(hostname=TS_URL, username=TS_USER, password=TS_PASSWORD)

        reviewer = DataModelReviewer()
        issues = reviewer.review_model(database=database, rtql=rtql)
        self.assertEqual(6, len(issues["review_sharding"]))

    def test_primary_keys(self):
        """Tests the review of primary keys."""

        parser = DDLParser(database_name="review_pks")
        database = parser.parse_ddl("no_pks.tql")

        reviewer = DataModelReviewer()
        issues = reviewer.review_model(database=database)
        self.assertEqual(1, len(issues["review_pks"]))

    def test_table_joins(self):
        """Tests the review of the joins between tables in a database."""
        parser = DDLParser(database_name="table_join_test")
        database = parser.parse_ddl("table_joins.tql")
        rtql = RemoteTQL(hostname=TS_URL, username=TS_USER, password=TS_PASSWORD)

        reviewer = DataModelReviewer()
        issues = reviewer.review_model(test_names=["review_table_joins"], database=database, rtql=rtql)
        self.assertEqual(2, len(issues['review_table_joins']))

    def test_worksheet_join_types(self):
        """Tests the review of the joins between tables in  worksheet.."""

        parser = DDLParser(database_name="golf_sales")
        database = parser.parse_ddl("test_data/golf_sales/golf_sales.tql")
        ws_reader = YAMLWorksheetReader()
        worksheet = ws_reader.read_from_file("test_data/golf_sales/Golf Sales WS.yaml")
        rtql = RemoteTQL(hostname=TS_URL, username=TS_USER, password=TS_PASSWORD)

        reviewer = DataModelReviewer()
        issues = reviewer.review_model(database=database, worksheet=worksheet, rtql=rtql)
        self.assertEqual(3, len(issues["review_worksheet_joins"]))
