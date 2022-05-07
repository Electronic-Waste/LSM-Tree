
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: kvstore.o correctness.o memtable.o

persistence: kvstore.o persistence.o memtable.o

clean:
	-rm -f correctness persistence *.o
