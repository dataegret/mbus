EXTENSION = mbus
PGFILEDESC = "mbus - Lightweight PostgreSQL extension for asynchronous messaging"

PG_CONFIG = pg_config
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
DATA = $(wildcard *--*.sql)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
