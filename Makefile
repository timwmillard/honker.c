CC      = cc
CFLAGS  = -O2 -Wall -Wextra

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
  LIB = libhonker.dylib
  LDFLAGS = -dynamiclib -undefined dynamic_lookup
  # Use the Homebrew SQLite headers so the extension ABI matches Python's sqlite3.
  SQLITE_INC := $(shell ls -d /opt/homebrew/Cellar/sqlite/*/include 2>/dev/null | sort -V | tail -1)
  ifneq ($(SQLITE_INC),)
    CFLAGS += -I$(SQLITE_INC)
  endif
else
  LIB = libhonker.so
  LDFLAGS = -fPIC -shared
endif

.PHONY: all clean test pytest

all: $(LIB)

$(LIB): honker.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

test_honker: test_honker.c $(LIB)
	$(CC) $(CFLAGS) -I/opt/local/include -o $@ test_honker.c -L/opt/local/lib -lsqlite3

test: test_honker
	./test_honker

pytest: $(LIB)
	.venv/bin/pytest

clean:
	rm -f $(LIB) test_honker
