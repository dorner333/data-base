#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "hashdb.h"

// для дебага тестов
//#define DEBUG
// режим эксперемента
//#define STAT

int main(int argc, char* argv[])
{
    DB* dbh;
    char buf[4096] = {};
    long long ftime = 0;
    double simple_time = 0;

    #ifdef STAT
    FILE* data = fopen("data.txt", "a");
    #endif

    if (argc < 2)
    {
        printf("usage: %s <dbfilename>\n", argv[0]);
        exit(1);
    }
    uint32_t (*hash)(const uint8_t* v);
    hash =  (hash_open(argv[2]));
    dbh = ht_open(argv[1], 141307, hash);
    if ( !dbh ) {
        printf("Cannot open database %s\n", argv[1]);
        exit(1);
    }

    // simple menu
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    hash_print(dbh);

    #ifndef STAT
    printf("Choose an action:\n\n");

    printf("Exit or QUIT            -- to exit\n");
    printf("MASS /*sart*/ /*count*/ -- to fill data base by values\n");
    printf("SET /*key*/             -- to set value by key\n");
    printf("GET /**/                -- to set value by key\n");
    printf("STAT                    -- to get a data base statistic\n");
    printf("DEL /*key*/             -- to delite value by key\n\n");
    #endif
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

                //Определяем текущее время
                clock_gettime(CLOCK_REALTIME, &mt1);
                #ifdef DEBUG
                //Выводим определенное время на экран консоли
                printf ("seconds: %ld\n", mt1.tv_sec);
                printf ("nano seconds: %ld\n", mt1.tv_nsec);
                #endif

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

                //Определяем текущее время
                clock_gettime (CLOCK_REALTIME, &mt2);
                //Рассчитываем разницу времени между двумя измерениями
                ftime = 1000000000*(mt2.tv_sec - mt1.tv_sec)+(mt2.tv_nsec - mt1.tv_nsec);
                simple_time = (double) ftime/1000000; //milliseconds

                #ifndef STAT
                printf("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                printf("TIME DUMP [HT_GET]\n");
                printf("ht_get function real time: %.2lf millisecons = %lld nanoseconds\n", simple_time, ftime);
                printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n");
                #else
                fprintf(data, "%.2lf ", simple_time);
                sleep(3);
                #endif
            }
            else if ( !strcmp(cmd, "DEL"))
            {
                char* key = strsep(&s, " \t\n");

                //Определяем текущее время
                clock_gettime(CLOCK_REALTIME, &mt1);
                #ifdef DEBUG
                //Выводим определенное время на экран консоли
                printf ("seconds: %ld\n", mt1.tv_sec);
                printf ("nano seconds: %ld\n", mt1.tv_nsec);
                #endif

                if ( key )
                {
                    ht_del(dbh, key);
                    printf("OK\n");
                }

                //Определяем текущее время
                clock_gettime (CLOCK_REALTIME, &mt2);
                //Рассчитываем разницу времени между двумя измерениями
                ftime = 1000000000*(mt2.tv_sec - mt1.tv_sec)+(mt2.tv_nsec - mt1.tv_nsec);
                simple_time = (double) ftime/1000000; //milliseconds

                #ifndef STAT
                printf("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                printf("TIME DUMP [HT_DEL]\n");
                printf("ht_del function real time: %.2lf millisecons = %lld nanoseconds\n", simple_time, ftime);
                printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n");
                #else
                fprintf(data, "%.2lf ", simple_time);
                sleep(3);
                #endif

            }
            else if ( !strcmp(cmd, "STAT"))
            {
                Stat stat;
                ht_get_stat(dbh, &stat);
                #ifndef STAT
                    printf("Keys: %lu\n"
                        "tables: %u\n"
                        "Avg. key size: %f\n"
                        "Avg. value size: %f\n"
                        "Total capacity: %lu\n"
                        "Avg. Fill factor: %f\n"
                        "Max chain length: %u\n"
                        "Filled nodes: %lu\n"
                        "Node fill factor: %f\n"
                        "Avg chain length: %f\n",
                        stat.keys, 
                        stat.tables,
                        stat.keysz/(float)stat.keys,
                        stat.valuesz/(float)stat.keys,
                        stat.capacity,
                        stat.keys/(float)stat.capacity,
                        stat.maxlen,
                        stat.nodes,
                        stat.nodes/(float)stat.capacity,
                        stat.keys/(float)stat.nodes);
                #else
                    fprintf(data, "%0.3f ", stat.keysz/(float)stat.keys);
                    sleep(1);
                #endif
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
                    #ifdef DEBUG
                    //Выводим определенное время на экран консоли
                    printf ("seconds: %ld\n", mt2.tv_sec);
                    printf ("nano seconds: %ld\n", mt2.tv_nsec);
                    #endif
                    //Рассчитываем разницу времени между двумя измерениями
                    ftime = 1000000000*(mt2.tv_sec - mt1.tv_sec)+(mt2.tv_nsec - mt1.tv_nsec);
                    simple_time = ftime / cnt; 
                    simple_time = simple_time / 1000000; // millisecons

                    #ifndef STAT
                    printf("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                    printf("TIME DUMP [HT_SET]\n");
                    printf("ht_set function real time: %lld nanosecons\n", ftime);
                    printf("time to set one pair k-v: %.2lf milliseconds = %lld nanoseconds\n", simple_time, ftime/cnt);
                    printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n");
                    #else
                    fprintf(data, "%.2lf ", simple_time);
                    sleep(3);
                    #endif
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

    #ifdef STAT
    fprintf(data, "\n");
    fclose(data);
    #endif 
    
    return 0;
}

void* hash_open(char* hash_name)
    {
    uint32_t (*hash)(const uint8_t* v);

    if (!strcmp(hash_name,"murmur3"))
        {
        hash = murmur3_32;
        }
    else if (!strcmp(hash_name,"murmur2"))
        {
        hash = murmur2_32;
        }
    else if (!strcmp(hash_name,"murmur3_32"))
        {
        hash = murmur3_32;
        }
    else if (!strcmp(hash_name,"murmur2_32"))
        {
        hash = murmur2_32;
        }
    else if (!strcmp(hash_name,"CRC32"))
        {
        hash = CRC32;
        }
    else if (!strcmp(hash_name,"crc32"))
        {
        hash = CRC32;
        }
    else if (!strcmp(hash_name,"fnv32"))
        {
        hash = FNV32;
        }
    else if (!strcmp(hash_name,"FNV32"))
        {
        hash = FNV32;
        }
    else if (!strcmp(hash_name,"rot1333"))
        {
        hash = rot1333;
        }
    else
        {
        printf("!ERROR! BAD INPUT HASH FUNCTION NAME\n USING murmur3_32\n");   
        hash = murmur3_32;
        }

    return (void*) hash;
    }

void hash_print(DB* dbh)
    {
    if (dbh -> hash == rot1333)
        {
        printf("hash function - rot1333\n\n");
        }
    else if (dbh -> hash == murmur3_32)
        {
        printf("hash function - murmur3_32\n\n");
        }
    else if (dbh -> hash == murmur2_32)
        {
        printf("hash function - murmur2_32\n\n");
        }
    else if (dbh -> hash == CRC32)
        {
        printf("hash function - CRC32\n\n");
        }
    else if (dbh -> hash == FNV32)
        {
        printf("hash function - FNV32\n\n");
        }      
    else 
        {
        printf("hash function - NULL(error)\n\n");
        }
    }

//! WARNIN: при использовании функции MASS программа начинает выдавать [err] на значении счетчика 170000
//! ввести 1000000 за раз я так и не смог (не дождался)

//TODO: Паша
//* добавил подсчет времени работы функции ht_set
//* добавил подсчет времени работы функции ht_get
//* добавил подсчет времени работы функции DEl
//* добавил вывод менюшки действий при запуске
//* сделал черновой вариант тестирования

//* сделал в hashdbtest мод STAT для записи экспеперемнтальных данных в файл
//* сделал bush script для автоматического сбора данных
