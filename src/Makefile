all: libmongodb.a

SOURCES = $(wildcard *.c)
OBJS = $(patsubst %.c, %.o, $(SOURCES))

libmongodb.a: $(OBJS)
	@rm -rf $@
	ar cr $@ $(OBJS)

%.o:%.c
	gcc --std=c99 -fPIC -D_GNU_SOURCE -c $< -o $@

clean:
	@rm -rf $(OBJS) *.a
