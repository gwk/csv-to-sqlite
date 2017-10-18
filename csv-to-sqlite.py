#!/usr/bin/env python3
# Dedicated to the public domain under CC0: https://creativecommons.org/publicdomain/zero/1.0/.

import sys
assert sys.version_info >= (3, 6, 0)

import csv
import re
import readline # by importing this module, `input` function gains history capabilities.
import sqlite3
from argparse import ArgumentParser
from collections import Counter
from sqlite3 import Cursor, OperationalError, connect, complete_statement as is_complete_sqlite_statement
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

  try: db = DB(args.output)
  except OperationalError as e:
    if e.args[0] in {'unable to open database file'}:
      exit(f'error: {args.output}: {e.args[0]}.')
    raise #!cov-ignore.

  pairs = args.csv_table_pairs
  if len(pairs) % 2:
    exit(f'error: uneven number of (csv_path, table_name) pairs.')

  db.enable_wal_mode()

  for csv_path, table in zip(pairs[0::2], pairs[1::2]):
    load_table(db=db, csv_path=csv_path, table=table, dialect=dialect)

  if not args.output:
    db.interactive_session()


def load_table(db, csv_path, table, dialect):
  table = clean_sym(table, 'table name')

  try: f = open(csv_path, newline='') # newline arg is recommended by csv.reader docs.
  except FileNotFoundError as e: exit(e)

  # If a UTF8 BOM is present, remove it; for now we assume UTF8.
  # The UTF8 BOM is '\uFEFF' / b'\xef\xbb\xbf'.
  # Note that TextIOWrapper.read parameter units are in characters, but `seek` and `tell` are in bytes.
  lead_char = f.read(1)
  if lead_char == '\uFEFF':
    start_offset = f.tell()
  else:
    start_offset = 0
    f.seek(0)

  # Infer column affinities (SQLite terminology for weak types).
  header, reader = header_reader(f, dialect)
  col_names, columns = infer_columns(header, reader)
  errSL(f'schema for `{table}`:', *[f'{n}:{a}' for n, a in columns])

  # Read the file again and insert the rows.
  f.seek(start_offset)
  _, reader = header_reader(f, dialect)
  db.drop_and_create_table(table, columns)
  db.insert_rows(table, col_names, reader)


def header_reader(f, dialect):
  reader = csv.reader(f, dialect=dialect)
  try: header = next(reader)
  except StopIteration: exit('error: empty csv input.')
  return header, reader


def infer_columns(header, reader):

  used_names = set()
  def unique_name(name):
    s = clean_sym(name, 'column name')
    i = 0
    si = s # omit index for 0 case.
    while si.lower() in used_names:
      i += 1
      si = f'{s}_{i}'
    used_names.add(si.lower())
    return si

  col_names = [unique_name(name) for name in header]
  col_count = len(col_names)
  column_states = [S_NONE for _ in col_names]

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
  return col_names, tuple(zip(col_names, affinities))


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

  def enable_wal_mode(self):
    self.run('PRAGMA journal_mode=WAL')

  def drop_and_create_table(self, table, columns):
    #db_columns = (('id', 'INTEGER PRIMARY KEY'),) + columns
    columns_decl = ', '.join(f'{n} {a}' for n, a in columns)
    self.run(f'DROP TABLE IF EXISTS {table}')
    self.run(f'CREATE TABLE IF NOT EXISTS {table} ({columns_decl})')

  def insert_rows(self, table, col_names, reader):
    names = ', '.join(col_names)
    placeholders = ', '.join('?' for _ in col_names)
    insert_stmt = f'INSERT INTO {table} ({names}) values ({placeholders})'
    try: self.conn.executemany(insert_stmt, reader)
    except Exception:
      errSL(insert_stmt)
      raise


  def interactive_session(self):

    def execute(cmd):
      try:
        c = self.run(cmd)
        print(*c.fetchall(), sep='\n')
      except sqlite3.Error as e:
        errSL('sqlite error:', *e.args)

    buffer = ''
    while True:
      try:
        line = input('> ')
        buffer += line
        if is_complete_sqlite_statement(buffer):
          execute(buffer.strip())
          buffer = ''
      except KeyboardInterrupt:
        print('^KeyboardInterrupt')
      except EOFError:
        print()
        return


def clean_sym(sym, desc):
  orig = sym
  sym = sym_re.sub('_', sym)
  if not sym: sym = '_'
  if sym[0].isnumeric(): sym = '_' + sym
  if sym != orig: errSL(f'note: {desc} converted from {orig!r} to {sym!r}')
  return sym

sym_re = re.compile(r'[^\w]', flags=re.ASCII)

sqlite_keywords = {
  'ABORT',
  'ACTION',
  'ADD',
  'AFTER',
  'ALL',
  'ALTER',
  'ANALYZE',
  'AND',
  'AS',
  'ASC',
  'ATTACH',
  'AUTOINCREMENT',
  'BEFORE',
  'BEGIN',
  'BETWEEN',
  'BY',
  'CASCADE',
  'CASE',
  'CAST',
  'CHECK',
  'COLLATE',
  'COLUMN',
  'COMMIT',
  'CONFLICT',
  'CONSTRAINT',
  'CREATE',
  'CROSS',
  'CURRENT_DATE',
  'CURRENT_TIME',
  'CURRENT_TIMESTAMP',
  'DATABASE',
  'DEFAULT',
  'DEFERRABLE',
  'DEFERRED',
  'DELETE',
  'DESC',
  'DETACH',
  'DISTINCT',
  'DROP',
  'EACH',
  'ELSE',
  'END',
  'ESCAPE',
  'EXCEPT',
  'EXCLUSIVE',
  'EXISTS',
  'EXPLAIN',
  'FAIL',
  'FOR',
  'FOREIGN',
  'FROM',
  'FULL',
  'GLOB',
  'GROUP',
  'HAVING',
  'IF',
  'IGNORE',
  'IMMEDIATE',
  'IN',
  'INDEX',
  'INDEXED',
  'INITIALLY',
  'INNER',
  'INSERT',
  'INSTEAD',
  'INTERSECT',
  'INTO',
  'IS',
  'ISNULL',
  'JOIN',
  'KEY',
  'LEFT',
  'LIKE',
  'LIMIT',
  'MATCH',
  'NATURAL',
  'NO',
  'NOT',
  'NOTNULL',
  'NULL',
  'OF',
  'OFFSET',
  'ON',
  'OR',
  'ORDER',
  'OUTER',
  'PLAN',
  'PRAGMA',
  'PRIMARY',
  'QUERY',
  'RAISE',
  'RECURSIVE',
  'REFERENCES',
  'REGEXP',
  'REINDEX',
  'RELEASE',
  'RENAME',
  'REPLACE',
  'RESTRICT',
  'RIGHT',
  'ROLLBACK',
  'ROW',
  'SAVEPOINT',
  'SELECT',
  'SET',
  'TABLE',
  'TEMP',
  'TEMPORARY',
  'THEN',
  'TO',
  'TRANSACTION',
  'TRIGGER',
  'UNION',
  'UNIQUE',
  'UPDATE',
  'USING',
  'VACUUM',
  'VALUES',
  'VIEW',
  'VIRTUAL',
  'WHEN',
  'WHERE',
  'WITH',
  'WITHOUT',
}


def errSL(*args): print(*args, file=stderr)


if __name__ == '__main__': main()
