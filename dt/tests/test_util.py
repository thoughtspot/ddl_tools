"""
Copyright 2020 ThoughtSpot

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

General utilities for DDL tools.
"""
import unittest
import os

from dt.util import ConfigFile


class TestConfigFile(unittest.TestCase):
    """ Tests the configuration file class."""
    TEST_CONFIG="test.cfg"

    def setUp(self) -> None:
        """Creates a test file."""
        with open(TestConfigFile.TEST_CONFIG, "w") as test_file:
            test_file.write("# this is a test file.\n")
            test_file.write("key1=val1 # no spaces.\n")
            test_file.write("key2 =val2 # space before.\n")
            test_file.write("key3= val3 # space after.\n")
            test_file.write(" key4 = val4# space before key.\n")
            test_file.write(" key5 = # missing value.\n")
            test_file.write(" = val6 # missing key.\n")
            test_file.write(" key7 is val7 # no equal, so ignore..\n")

    def tearDown(self) -> None:
        """Deletes the test file."""
        os.remove(TestConfigFile.TEST_CONFIG)

    def test_create_config_file(self):
        cf = ConfigFile()
        self.assertEqual(len(cf), 0)

    def test_load_from_file(self):
        """Tests loading values from a file."""
        cf = ConfigFile()
        cf.load_from_file(TestConfigFile.TEST_CONFIG)

        self.assertEqual(4, len(cf))
        self.assertEqual(cf["key1"], "val1")
        self.assertEqual(cf["key2"], "val2")
        self.assertEqual(cf["key3"], "val3")
        self.assertEqual(cf["key4"], "val4")

    def test_write_to_file(self):
        """Test writing the __config to a file."""
        test_filename = "test_write.cf"
        cf1 = ConfigFile()
        cf1["key1"] = "val1"
        cf1["key2"] = "val2"
        cf1["key3"] = "val3"
        cf1.write_to_file(filename=test_filename)

        cf2 = ConfigFile()
        cf2.load_from_file(filename=test_filename)

        self.assertTrue(cf1 == cf2)

    def test_get(self):
        """Tests using the get method."""
        cf = ConfigFile()
        cf["k1"] = "v1"

        self.assertEqual(cf.get("k1"), "v1")
        self.assertEqual(cf.get("k2"), None)
        self.assertEqual(cf.get("k3", default=3), 3)


