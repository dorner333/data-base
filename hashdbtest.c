#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "hashdb.h"

// для дебага тестов
#define DEBUG

int main(int argc, char* argv[])
{
    DB* dbh;
    char buf[4096] = {};
    if (argc < 2)
    {
        printf("usage: %s <dbfilename>\n", argv[0]);
        exit(1);
    }
    dbh = ht_open(argv[1], 141307);
    if ( !dbh ) {
        printf("Cannot open database %s\n", argv[1]);
        exit(1);
    }
    while(fgets(buf, 4096, stdin))
    {
        char*s = buf;
        char* cmd = strsep(&s, " \t\n");
        if ( cmd ) {
            if (!strcmp(cmd, "SET"))
            {
                char* key = strsep(&s, " \t\n");
                char* val = strsep(&s, "\n");
                if (key && val)
                {
                    printf("Setting '%s' = '%s'\n", key, val);
                    ht_set(dbh, key, val);
                    printf("OK\n");
                }
                else
                    printf("No key or val given\n");
            }
            else if ( !strcmp(cmd, "GET"))
            {
                char* key = strsep(&s, " \t\n");
                if ( key )
                {
                    char* val;
                    if ( ht_get(dbh, key, &val) )
                    {
                        printf("%s\n", val);
                        free(val);
                    } else
                    {
                        printf("No such key\n");
                    }
                }
            }
            else if ( !strcmp(cmd, "DEL"))
            {
                char* key = strsep(&s, " \t\n");
                if ( key )
                {
                    ht_del(dbh, key);
                    printf("OK\n");
                }
            }
            else if ( !strcmp(cmd, "STAT"))
            {
                Stat stat;
                ht_get_stat(dbh, &stat);
                printf("Keys: %lu\n tables: %u\nAvg. key size: %f\n"
                    "Avg. value size: %f\n"
                    "Total capacity: %lu\n"
                    "Avg. Fill factor: %f\n"
                    "Max chain length: %u\n"
                    "Filled nodes: %lu\n"
                    "Node fill factor: %f\n"
                    "Avg chain length: %f\n",
                    stat.keys, stat.tables,
                    stat.keysz/(float)stat.keys,
                    stat.valuesz/(float)stat.keys,
                    stat.capacity,
                    stat.keys/(float)stat.capacity,
                    stat.maxlen,
                    stat.nodes,
                    stat.nodes/(float)stat.capacity,
                    stat.keys/(float)stat.nodes);
            }
            else if ( !strcmp(cmd, "MASS"))
            {
                char *start = strsep(&s, " \t\n");
                char *count = strsep(&s, " \t\n");
                if (start && count)
                {
                    int i;
                    int st = atoi(start);
                    int cnt = atoi(count);
                    printf("Starting set from %d to %d\n", st, st+cnt);

                    //Определяем текущее время
                    clock_gettime(CLOCK_REALTIME, &mt1);
                    #ifdef DEBUG
                    //Выводим определенное время на экран консоли
                    printf ("seconds: %ld\n", mt1.tv_sec);
                    printf ("nano seconds: %ld\n", mt1.tv_nsec);
                    #endif

                    for ( i = st; i<= st+cnt; i++)
                    {
                        char key[100];
                        char value[100];
                        snprintf(key, 100, "key%d", i);
                        snprintf(value, 100, "value%d", i);
                        ht_set(dbh, key, value);
                    }
                    printf("Done\n");

                    //Определяем текущее время
                    clock_gettime (CLOCK_REALTIME, &mt2);
                    //Рассчитываем разницу времени между двумя измерениями
                    long long ftime = 1000000000*(mt2.tv_sec - mt1.tv_sec)+(mt2.tv_nsec - mt1.tv_nsec);
                    int simple_time = (int) ftime / cnt;
                    printf("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                    printf("TIME DUMP\n");
                    printf("ht_set function real time: %lld nanosecons\n", ftime);
                    printf("time to set one pair k-v: %d nanoseconds\n", simple_time);
                    printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n");
                }

            }
            else if ( !strcmp(cmd, "QUIT") || !strcmp(cmd, "EXIT"))
            {
                break;
            }
            else
            {
                printf("unknown command\n");
            }
        }
    }
    ht_close(dbh);
    return 0;
}
