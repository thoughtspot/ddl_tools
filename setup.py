from setuptools import setup, find_packages

setup(
    name='ddl_tools',
    version='1.0',
    python_requires='>3.6',
    description='This package contains tools for working with ThoughtSpot DDL.',
    long_description_content_type="text/markdown",
    url='https://thoughtspot.com',
    author='Bill Back',
    author_email='bill.back@thoughtspot.com',
    license='MIT',
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'xlrd',
        'openpyxl',
        'pyYAML',
        'py-tql@git+ssh://github.com/thoughtspot/py-tql/@master#egg=tql'
    ]
)
