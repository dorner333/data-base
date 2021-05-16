#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "hashdb.h"

#define _RED_                "\x1b[31;1m"
#define _BOLD_               "\x1b[1m"
#define _GREEN_              "\x1b[32;1m"
#define _LIGHT_BLUE_         "\x1b[36;1m"
#define _YELLOW_             "\x1b[33;1m"
#define _PINK_               "\x1b[35;1m"
#define _BLUE_               "\x1b[34;1m"
#define _RES_                "\x1b[0m"

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

    //FILE* command = fopen("instruction.txt", "a");
    //fgets(buf, 4096, command);
    #ifdef STAT
    FILE* data = fopen("data.txt", "a");
    #endif

    if (argc < 2)
    {
        printf("usage: %s <dbfilename> <hashfunction>\n", argv[0]);
        exit(1);
    }
    uint32_t (*hash)(const uint8_t* v);
    if (argc < 3) {
        hash = hash_open("rot1333");
    }
    else {
        hash =  (hash_open(argv[2]));
    }
    dbh = ht_open(argv[1], 141307, hash);
    if ( !dbh ) {
        printf("!!ERROR: Cannot open database %s!!\n", argv[1]);
        exit(1);
    }

    // simple menu
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #ifndef STAT
    printf("\n\n##################################################################################\n");
    printf("##                                                                              ##\n");
    printf("##                                                                              ##\n");
    #endif

    hash_print(dbh);

    #ifndef STAT
    printf("##                                                                              ##\n");
    printf("##                                                                              ##\n");
    printf("##    Choose an action:                                                         ##\n");
    printf("##                                                                              ##\n");
    printf("##    "_PINK_" MASS\x1b[0m "_RES_"              "_YELLOW_"/*"_GREEN_"sart"_YELLOW_"*/ /*"_GREEN_"count"_YELLOW_"*/ -- ""to fill data base by values"_RES_"     ##\n");
    printf("##    "_PINK_" SET\x1b[0m  "_RES_"              "_YELLOW_"/*"_GREEN_"key"_YELLOW_"*/  /*"_GREEN_"value"_YELLOW_"*/ -- to set value by key"_RES_"             ##\n");
    printf("##    "_PINK_" GET\x1b[0m  "_RES_"              "_YELLOW_"/*"_GREEN_"key"_YELLOW_"*/            -- to get value by key"_RES_"             ##\n");
    printf("##    "_PINK_" DEL\x1b[0m  "_RES_"              "_YELLOW_"/*"_GREEN_"key"_YELLOW_"*/            -- to delete value by key"_RES_"          ##\n");
    printf("##    "_PINK_" STAT\x1b[0m "_RES_"                                 "_YELLOW_"-- to get a data base statistic"_RES_"    ##\n");
    printf("##    "_PINK_" EXIT or QUIT "_RES_"                         "_YELLOW_"-- to exit"_RES_"                         ##\n");
    printf("##                                                                              ##\n");
    printf("##                                                                              ##\n");
    printf("##################################################################################\n\n");
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
                printf("\n##################################################################################\n");
                printf("##                                                                              ##\n");
                printf("##                                                                              ##\n");
                printf("##                "_LIGHT_BLUE_" TIME DUMP [HT_GET] "_RES_"                                          ##\n");
                printf("##                "_LIGHT_BLUE_" ht_get function real time: "_GREEN_"%8.2lf"_LIGHT_BLUE_" millisecons "_RES_"             ##\n",simple_time);
                printf("##                                                                              ##\n");
                printf("##                                                                              ##\n");
                printf("##################################################################################\n\n");
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
                printf("\n##################################################################################\n");
                printf("##                                                                              ##\n");
                printf("##                                                                              ##\n");
                printf("##                "_LIGHT_BLUE_" TIME DUMP [HT_DEL] "_RES_"                                          ##\n");
                printf("##                "_LIGHT_BLUE_" ht_del function real time: "_GREEN_"%8.2lf"_LIGHT_BLUE_" millisecons "_RES_"             ##\n", simple_time);
                printf("##                                                                              ##\n");
                printf("##                                                                              ##\n");
                printf("##################################################################################\n\n");
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
                    printf("\n\tKeys quantity:    "_GREEN_"%lu"_RES_"\n"
                           "\ttables quantity:  "_GREEN_"%u"_RES_"\n"
                           "\tAvg. key size:    "_GREEN_"%f"_RES_"\n"
                           "\tAvg. value size:  "_GREEN_"%f"_RES_"\n"
                           "\tTotal capacity:   "_GREEN_"%lu"_RES_"\n"
                           "\tAvg. Fill factor: "_GREEN_"%f"_RES_"\n"
                           "\tMax chain length: "_GREEN_"%u"_RES_"\n"
                           "\tFilled nodes:     "_GREEN_"%lu"_RES_"\n"
                           "\tNode fill factor: "_GREEN_"%f"_RES_"\n"
                           "\tAvg chain length: "_GREEN_"%f"_RES_"\n"
                           "\tCollision amount: "_GREEN_"%lu"_RES_"\n\n",
                        stat.keys,
                        stat.tables,
                        stat.keysz/(float)stat.keys,
                        stat.valuesz/(float)stat.keys,
                        stat.capacity,
                        stat.keys/(float)stat.capacity,
                        stat.maxlen,
                        stat.nodes,
                        stat.nodes/(float)stat.capacity,
                        stat.keys/(float)stat.nodes,
                        stat.collision_amount);
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
                    printf("\n##################################################################################\n");
                    printf("##                                                                              ##\n");
                    printf("##                                                                              ##\n");
                    printf("##                "_LIGHT_BLUE_" TIME DUMP [HT_SET] "_RES_"                                          ##\n");
                    printf("##                "_LIGHT_BLUE_" ht_set function real time: "_GREEN_"%7.2lld"_LIGHT_BLUE_" milliseconds  "_RES_"            ##\n", ftime/1000000);
                    printf("##                "_LIGHT_BLUE_" time to set one pair k-v: "_GREEN_"%8.2lf"_LIGHT_BLUE_" milliseconds "_RES_"             ##\n",simple_time);
                    printf("##                                                                              ##\n");
                    printf("##                                                                              ##\n");
                    printf("##################################################################################\n\n");
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
        //fclose(command);
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
    //#ifdef STAT
        printf("##    Hash function - "_BLUE_"");
    //#endif
    if (dbh -> hash == rot1333)
        {
        printf("rot1333              "_RES_"");
        }
    else if (dbh -> hash == murmur3_32)
        {
        printf("murmur3_32           "_RES_"");
        }
    else if (dbh -> hash == murmur2_32)
        {
        printf("murmur2_32           "_RES_"");
        }
    else if (dbh -> hash == CRC32)
        {
        printf("CRC32                "_RES_"");
        }
    else if (dbh -> hash == FNV32)
        {
        printf("FNV32                "_RES_"");
        }
    else
        {
        printf(""_RED_"NULL(error)          "_RES_"");
        }

    printf("                                     ##\n");
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
