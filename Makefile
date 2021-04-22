CFLAGS+=-O3

hashdbtest: hashdbtest.o hashdb.o
	$(CC) -o $@ $^

%.o:%.c
	$(CC) -c -O3 -o $@ $<
