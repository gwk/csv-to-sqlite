# Dedicated to the public domain under CC0: https://creativecommons.org/publicdomain/zero/1.0/.

# $@: The file name of the target of the rule.
# $<: The name of the first prerequisite.
# $^: The names of all the prerequisites, with spaces between them.
# %: A wildcard pattern.

.PHONY: _default clean cov install test typecheck

# First target of a makefile is the default.
_default: test typecheck

clean:
	rm -rf _build/*

cov:
	iotest -fail-fast -coverage

install:
	sudo cp csv-to-sqlite.py /usr/local/bin/csv-to-sqlite

install-link:
	sudo ln -s $$PWD/csv-to-sqlite.py /usr/local/bin/csv-to-sqlite

uninstall:
	sudo rm /usr/local/bin/csv-to-sqlite

test:
	iotest -fail-fast

typecheck:
	craft-py-check csv-to-sqlite.py
