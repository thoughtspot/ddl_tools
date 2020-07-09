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

import inspect
import sys

from dt.model import Database, Worksheet
from dt.util import ConfigFile
from pytql.tql import RemoteTQL

from .review_tests import *


class DataModelReviewer:
    """
    Reviews a database model and provides recommendations and warnings.
    """
    # known modules with tests.
    modules = ["dt.review.review_tests"]

    def __init__(self, config_file=None):
        """
        Creates a new reviewer.
        :param config_file: Name of the configuration file for tests.
        :type config_file: str
        """
        self.config = ConfigFile()
        if config_file:
            self.config.load_from_file(filename=config_file)

    def review_model(self, database, worksheet=None, rtql=None):
        """
        Reviews the model.  A a minimum a database is needed.
        If a worksheet is provided, worksheet specific reviews will be run and not the entire database.
        If an RTQL connection is provided, data checks will also be performed.
        :param database: The database to evaluate.
        :type database: Database
        :param worksheet: A specific worksheet to evaluate.  These can be more useful since most work and problems are
        at the worksheet level.
        :type worksheet: Worksheet
        :param rtql: A remote TQL reference for making data queries.
        :type rtql: RemoteTQL
        :return: Dictionary with test and list of recommendations.  Could be empty.
        :rtype: dict
        """

        assert database  # A database is the minimum for testing.

        recommendations = {}

        # Get the review items for databases.
        for module_name in DataModelReviewer.modules:
            module = sys.modules[module_name]
            functions = dir(module)
            for f in functions:
                if f.startswith("review_"):
                    func = getattr(module, f)
                    func_parameters = inspect.signature(func).parameters.keys()
                    params = {}
                    if "database" in func_parameters:
                        params["database"] = database
                    if "rtql" in func_parameters:
                        if rtql:
                            params["rtql"] = rtql
                        else:  # if RTQL is required, but not provided, don't call.
                            param_str = ", ".join(list(func_parameters))
                            print(f"Skipping {f}({param_str})")
                            continue
                    if "worksheet" in func_parameters:
                        if worksheet:
                            params["worksheet"] = worksheet
                        else:  # if worksheet is required, but not provided, don't call.
                            param_str = ", ".join(list(func_parameters))
                            print(f"Skipping {f}({param_str})")
                            continue
                    if "config_file" in func_parameters:
                        params["config_file"] = self.config

                    if len(params):
                        r = func(**params)
                        if r:
                            recommendations[f] = r

        return recommendations

    def get_test_descriptions(self):
        """
        Returns a dictionary of all the tests along with their descriptions and what they operate on.
        :return: Dictionary with test signature and description of the test as a list of strings.
        :rtype: dict
        """
        descriptions = {}

        # Get the review items for databases.
        for module_name in DataModelReviewer.modules:
            module = sys.modules[module_name]
            functions = dir(module)
            for f in functions:
                if f.startswith("review_"):
                    func = getattr(module, f)
                    func_parameters = inspect.signature(func).parameters.keys()
                    param_str = ", ".join(list(func_parameters))

                    description = []
                    doc = func.__doc__.split("\n")
                    doc = [d.strip() for d in doc]
                    for d in doc:
                        if not d:
                            continue
                        if d.startswith(":"):
                            break
                        description.append(d)

                    descriptions[f"{f}({param_str})"] = description

        return descriptions
