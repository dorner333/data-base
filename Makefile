all:
	gcc -c hashdb.c
	gcc -c hashdbtest.c
	gcc -o hashdbtest hashdbtest.o hashdb.o

test1:
	gcc -c hashdb.c
	gcc -c test1.c
	gcc -o test1 test1.o hashdb.o

Ofast:
	gcc -ftime-report -Ofast -c -o hashdb.o hashdb.c
	gcc -ftime-report -Ofast -c -o hashdbtest.o hashdbtest.c
	gcc -Ofast -o hashdbtest hashdbtest.o hashdb.o

profile:
	gcc -ftime-report -Ofast -c -o hashdb.o hashdb.c
	gcc -ftime-report -Ofast -c -o hashdbtest.o hashdbtest.c
	gcc -Ofast -o hashdbtest hashdbtest.o hashdb.o
	valgrind --tool=callgrind ./hashdbtest test

