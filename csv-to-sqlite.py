#!/usr/bin/env python3
# Dedicated to the public domain under CC0: https://creativecommons.org/publicdomain/zero/1.0/.

import sys
assert sys.version_info >= (3, 6, 0)

import csv
import re
import readline # by importing this module, `input` function gains history capabilities.
from argparse import ArgumentParser
from sqlite3 import Cursor, connect, complete_statement as is_complete_sqlite_statement
from sys import stdin, stdout, stderr
from typing import *


description = '''
Import CSV data into an SQLite 3 database table, inferring column affinity types from the data.
The Python `csv` module is used to read in the CSV data.

If no output path is specified, then a temporary in-memory database is created and on completion
the program enters into an interactive SQL session.
'''

epilog = '''
Note that the sqlite3 command line interpreter also supports importing CSV in an interactive session:
https://sqlite.org/cli.html#csv_import. For large datasets this is undoubtedly faster,
but no type detection is performed, and SQLite parses CSV according to RFC 4180.
Additionally, SQLite has a loadable extension called the CSV Virtual table: https://sqlite.org/csv.html.
'''

'''
TODO:
* `-append` option to not drop existing tables?
* columns:
  parser.add_argument('-columns', nargs='+', help='manually specify columns names.')
  parser.add_argument('-no-header', dest='has_header', action='store_false',
    help='Specify that the input CSV has no header row.')
'''


def main():
  dialects = csv.list_dialects()
  parser = ArgumentParser(prog='csv-to-sqlite', description=description, epilog=epilog)
  parser.add_argument('-output', default=None,
    help='path to the new or existing SQLite database (omit for a temporary in-memory DB)')
  parser.add_argument('-dialect', default='excel', help=f'the CSV dialect to use {dialects}')
  parser.add_argument('csv_table_pairs', nargs='+',
    help='consecutive pairs of (csv_path, table_name).')

  args = parser.parse_args()

  dialect = args.dialect
  if dialect not in dialects:
    exit(f'error: invalid CSV dialect: {dialect!r}.')

  if len(args.csv_table_pairs) % 2 != 0:
    exit(f'error: csv_path and table_name arguments must be specified in pairs.')

  db = DB(args.output)
  pairs = args.csv_table_pairs
  for csv_path, table in zip(pairs[0::2], pairs[1::2]):
    load_table(db=db, csv_path=csv_path, table=table, dialect=dialect)

  if not args.output:
    db.interactive_session()


def load_table(db, csv_path, table, dialect):
  validate_sym(table, 'table name')

  try: f = open(csv_path, newline='') # newline arg is recommended by csv.reader docs.
  except FileNotFoundError as e: exit(e)

  # Infer column affinities (SQLite terminology for weak types).
  header, reader = header_reader_for(f, dialect)
  columns = infer_columns(header, reader)
  errSL('schema:', *[f'{n}:{a}' for n, a in columns])

  # Read the file again and insert the rows.
  f.seek(0)
  header, reader = header_reader_for(f, dialect)
  db.drop_and_create_table(table, columns)
  db.insert_rows(table, header, reader)


def header_reader_for(f, dialect):
  reader = csv.reader(f, dialect=dialect)
  try: header = next(reader)
  except StopIteration: exit('error: empty csv input.')
  return header, reader


def infer_columns(header, reader):
  for name in header:
    validate_sym(name, 'column name')

  col_count = len(header)
  column_states = [S_NONE for _ in header]

  bad_rows = False
  for i, row in enumerate(reader, 1):
    if len(row) != col_count:
      errSL(f'error: row {i} has {len(row)} cells:', row)
      bad_rows = True
    for i, (state, cell) in enumerate(zip(column_states, row)):
      if not cell or state == S_TXT: continue
      column_states[i] = state_for(state, cell)
  if bad_rows: exit(1)
  affinities = [column_state_affinities[state] for state in column_states]
  return tuple(zip(header, affinities))


def state_for(state, cell):
  if state == S_NONE or state == S_INT:
    try: int(cell)
    except ValueError: pass
    else: return S_INT
  try: float(cell)
  except ValueError: pass
  else: return S_FLT
  return S_TXT

# column states.
S_NONE, S_INT, S_FLT, S_TXT = range(4)

column_state_affinities = {
  S_NONE: 'TEXT',
  S_INT: 'INTEGER',
  S_FLT: 'REAL',
  S_TXT: 'TEXT',
}


class DB:

  def __init__(self, path: str) -> None:
    self.conn = connect(path or ':memory:')
    self.conn.isolation_level = None # autocommit mode.

  def run(self, query: str, *qmark_args, **named_args: Any) -> Cursor:
    return self.conn.execute(query, qmark_args or named_args)

  def drop_and_create_table(self, table, columns):
    #db_columns = (('id', 'INTEGER PRIMARY KEY'),) + columns
    columns_decl = ', '.join(f'{n} {a}' for n, a in columns)
    self.run(f'DROP TABLE IF EXISTS {table}')
    self.run(f'CREATE TABLE IF NOT EXISTS {table} ({columns_decl})')

  def insert_rows(self, table, header, reader):
    names = ', '.join(header)
    placeholders = ', '.join('?' for _ in header)
    insert_stmt = f'INSERT INTO {table} ({names}) values ({placeholders})'
    self.conn.executemany(insert_stmt, reader)


  def interactive_session(self):

    def execute(cmd):
      try:
        c = self.run(cmd)
        print(*c.fetchall(), sep='\n')
      except sqlite3.Error as e:
        errSL('sqlite error:', *e.args)

    buffer = ''
    while True:
      try: line = input('> ')
      except EOFError:
        print()
        return
      buffer += line
      if is_complete_sqlite_statement(buffer):
        execute(buffer.strip())
        buffer = ''


def validate_sym(sym, desc):
  if not sym_re.fullmatch(sym):
    exit(f'{desc} is not a valid SQLite identifier: {sym!r}')
  if sym.lower() in sqlite_reserved_names:
      exit(f'{desc} is a reserved SQLite identifier: {sym!r}')

sym_re = re.compile(r'\w+') # TODO: exact sqlite syntax.

sqlite_reserved_names = {
  'table',
  'values'
  # TODO: add the rest.
}


def errSL(*args): print(*args, file=stderr)

if __name__ == '__main__': main()
