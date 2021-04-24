All:
	gcc -c -o hashdb.o hashdb.c
	gcc -c -o hashdbtest.o hashdbtest.c
	gcc -o hashdbtest hashdbtest.o hashdb.o

details:
	gcc -ftime-report -Ofast -c -o hashdb.o hashdb.c
	gcc -ftime-report -Ofast -c -o hashdbtest.o hashdbtest.c
	gcc -Ofast -o hashdbtest hashdbtest.o hashdb.o

profile:
	gcc -ftime-report -Ofast -c -o hashdb.o hashdb.c
	gcc -ftime-report -Ofast -c -o hashdbtest.o hashdbtest.c
	gcc -Ofast -o hashdbtest hashdbtest.o hashdb.o
	valgrind --tool=callgrind ./hashdbtest test

