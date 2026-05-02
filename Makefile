CC      = cc
CFLAGS  = -O2 -Wall -Wextra

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
  LIB = libhonker.dylib
  LDFLAGS = -dynamiclib -undefined dynamic_lookup
else
  LIB = libhonker.so
  LDFLAGS = -fPIC -shared
endif

.PHONY: all clean test

all: $(LIB)

$(LIB): honker.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

test_honker: test_honker.c $(LIB)
	$(CC) $(CFLAGS) -I/opt/local/include -o $@ test_honker.c -L/opt/local/lib -lsqlite3

test: test_honker
	./test_honker

clean:
	rm -f $(LIB) test_honker
