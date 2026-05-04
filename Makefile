CC      = cc
CFLAGS  = -O2 -Wall -Wextra

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
  LIB     = libhonker.dylib
  LDFLAGS = -dynamiclib -undefined dynamic_lookup
else
  LIB     = libhonker.so
  LDFLAGS = -fPIC -shared
endif

SQLITE_VERSION = 3530000
SQLITE_YEAR    = 2026
SQLITE_URL     = https://www.sqlite.org/$(SQLITE_YEAR)/sqlite-amalgamation-$(SQLITE_VERSION).zip
SQLITE_INC     = libs
SQLITE_SRC     = libs/sqlite3.c

.PHONY: all clean test pytest sqlite examples

all: $(LIB)

# Download the SQLite amalgamation into libs/.
libs/sqlite3.h:
	mkdir -p libs
	curl -fsSL -o /tmp/sqlite-amal.zip $(SQLITE_URL)
	cd libs && unzip -jo /tmp/sqlite-amal.zip
	rm /tmp/sqlite-amal.zip

sqlite: libs/sqlite3.h

$(LIB): honker.c libs/sqlite3.h
	$(CC) $(CFLAGS) $(LDFLAGS) -I$(SQLITE_INC) -o $@ $<

test_honker: test_honker.c libs/sqlite3.h $(LIB)
	$(CC) $(CFLAGS) -DSQLITE_ENABLE_LOAD_EXTENSION -I$(SQLITE_INC) \
	    -o $@ test_honker.c $(SQLITE_SRC)

test: test_honker
	./test_honker

pytest: $(LIB)
	.venv/bin/pytest

examples/worker_queue: examples/worker_queue.c libs/sqlite3.h
	$(CC) $(CFLAGS) -DSQLITE_ENABLE_LOAD_EXTENSION -I$(SQLITE_INC) \
	    -o $@ examples/worker_queue.c $(SQLITE_SRC)

examples/pubsub: examples/pubsub.c libs/sqlite3.h
	$(CC) $(CFLAGS) -DSQLITE_ENABLE_LOAD_EXTENSION -I$(SQLITE_INC) \
	    -o $@ examples/pubsub.c $(SQLITE_SRC)

examples/stream_replay: examples/stream_replay.c libs/sqlite3.h
	$(CC) $(CFLAGS) -DSQLITE_ENABLE_LOAD_EXTENSION -I$(SQLITE_INC) \
	    -o $@ examples/stream_replay.c $(SQLITE_SRC)

examples/scheduler: examples/scheduler.c libs/sqlite3.h
	$(CC) $(CFLAGS) -DSQLITE_ENABLE_LOAD_EXTENSION -I$(SQLITE_INC) \
	    -o $@ examples/scheduler.c $(SQLITE_SRC)

examples: examples/worker_queue examples/pubsub examples/stream_replay examples/scheduler

clean:
	rm -f $(LIB) test_honker examples/worker_queue examples/pubsub examples/stream_replay examples/scheduler
