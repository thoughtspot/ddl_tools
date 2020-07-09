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
import sys

# -------------------------------------------------------------------------------------------------------------------


def eprint(*args, **kwargs):
    """
    Prints to standard error similar to regular print.
    :param args:  Positional arguments.
    :param kwargs:  Keyword arguments.
    """
    print(*args, file=sys.stderr, **kwargs)

# -------------------------------------------------------------------------------------------------------------------


class ConfigFile:
    """
    Simple configuation file that allows name / value pairs.  The pairs can be read from and written to a file and then
    used by the code.

    The file can have configuration items defined as: name=value
    The file can also have comments, which start with a hash symbol (#).  The can be anywhere and everything after is
    ignored.
    The file can also have blank lines.
    Any lines that have non-comments / non-blanks and no equal sign (=) will be ignored with a warning.
    """

    def __init__(self):
        """
        Creates a new ConfigFile.
        """
        self.__config = {}

    def get(self, name, default=None):
        """
        Gets the configuration value with the given name.
        :param name: Name of the item to get.
        :type name: str
        :param default: Default if the value doesn't exist.
        :type default: object
        :return: The value if it exists or the default if it doesn't.
        :rtype: Any
        """
        return self.__config.get(name, default)

    def __setitem__(self, key, value):
        """
        Sets the configuration item with the [] notation, e.g. cf[key] = value.
        :param key: The key to use.
        :param value:  The value to set.
        :return: None
        """
        self.__config[key] = value

    def __getitem__(self, item):
        """
        Returns the value for the item or None if it doesn't exist.
        :param item: Configuration item to get.
        :return: The value or None.
        """
        return self.get(name=item, default=None)

    def __len__(self):
        """
        Returns the number of configuration items.
        :return: The number of items.
        :rtype: int
        """
        return len(self.__config.keys())

    def __eq__(self, other):
        """
        Returns true of the two have equal configuration items.
        :param other:  The other configuration to compare to.
        :type other: ConfigFile
        :return: True if equal.
        """
        return self.__config == other.__config

    def load_from_file(self, filename):
        """
        Loads the __config from a file.
        :param filename: Path to the file to load from.
        :type filename: str
        :return: True if the configuration was successfully loaded.
        :rtype: bool
        """
        try:
            with open(filename, "r") as config_file:
                for line in config_file:
                    line = line.split("#")[0]  # Get rid of everything after a #.
                    line = line.strip()
                    if line != "" and "=" in line:  # actual configuration value.
                        key, value = [token.strip() for token in line.split("=", maxsplit=1)]
                        if key and value:  # make sure to ignore  xxx= or =xxx types of entries.
                            self.__config[key] = value
                        else:
                            eprint(f"{filename}: badly formed line '{line}'")
                    elif line != "":  # text but no =
                        eprint(f"{filename}: ignoring line '{line}'")

            return True
        except IOError:
            eprint(f"Unable to open file {filename}.  No configuration loaded.")
            return False

    def write_to_file(self, filename):
        """
        Writes the __config to a file.
        :param filename: Path to the file to write to.
        :type filename: str
        :return: True if the configuration was successfully written.
        :rtype: bool
        """
        try:
            with open(filename, "w") as config_file:
                for key, value in self.__config.items():
                    config_file.write(f"{key} = {value}\n")
            return True
        except IOError:
            eprint(f"Unable to open file {filename}.  No configuration written.")
            return False

