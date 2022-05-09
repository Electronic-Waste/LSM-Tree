
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: kvstore.o correctness.o sstable.o memtable.o bloomfilter.o

persistence: kvstore.o persistence.o sstable.o memtable.o bloomfilter.o

clean:
	-rm -f correctness persistence *.o
