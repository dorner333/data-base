All:
	gcc -c -o hashdb.o hashdb.c
	gcc -c -o hashdbtest.o hashdbtest.c
	gcc -o hashdbtest hashdbtest.o hashdb.o
