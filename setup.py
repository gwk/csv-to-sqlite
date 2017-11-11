# Dedicated to the public domain under CC0: https://creativecommons.org/publicdomain/zero/1.0/.


# users should install with: `$ pip3 install csv-to-sqlite`
# developers can make a local install with: `$ pip3 install -e .`
# upload to pypi test server with: `$ python3 setup.py sdist upload -r pypitest`
# upload to pypi prod server with: `$ python3 setup.py sdist upload`

from setuptools import setup


setup(
  name='csv-to-sqlite',
  license='CC0',
  version='0.0.1',
  author='George King',
  author_email='george.w.king@gmail.com',
  url='https://github.com/gwk/csv-to-sqlite',
  description='Convert CSV files into SQLite databases.',
  packages=['csv_to_sqlite'],
  entry_points = {'console_scripts': [
    'csv-to-sqlite=csv_to_sqlite.__main__:main',
  ]},
  keywords=[
    'sqlite', 'sql', 'database', 'CSV', 'excel',
  ],
  classifiers=[ # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: Education',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication',
    'Programming Language :: Python :: 3 :: Only',
    'Topic :: Database',
    'Topic :: Office/Business :: Financial :: Spreadsheet',
    'Topic :: Scientific/Engineering :: Information Analysis',
  ],
)
