all: bin/tcp-echo-server

install: bin/tcp-echo-server
	cp bin/tcp-echo-server /bin/

bin/tcp-echo-server: tcp-echo-server.c
	cc tcp-echo-server.c -o bin/tcp-echo-server

ht:
	gcc -c hashdb.c
	gcc -c hashdbtest.c
	gcc -o hashdbtest hashdbtest.o hashdb.o
	
clean:
	rm bin/*
