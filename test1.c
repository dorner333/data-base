#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "hashdb.h"

#define KEY_AMOUNT 1000

int main(int argc, char* argv[])
{
    DB* dbh;
    char buf[4096] = {};

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

    //* тестирование SET
    // заполняем базу
    for (int i = 0; i <= KEY_AMOUNT; i++)
    {
        char key[100];
        char value[100];
        snprintf(key, 100, "key%d", i);
        snprintf(value, 100, "value%d", i);
        ht_set(dbh, key, value);
    }

    for (int i = 0; i <= KEY_AMOUNT; i++) {
        char key[100];
        char value[100];
        snprintf(key, 100, "key%d", i);
        snprintf(value, 100, "value%d", i);

        char* val;
        if ( ht_get(dbh, key, &val) && (strcmp(val, value) == 0)){
            printf("key%d : OK\n", i);
            free(val);
        } else{
            printf("ERROR : key%d\nvalue%d = (%s) instead of (%s)\n", i, i, val, value);
            //exit(2);
        }
    }
    printf("GOOD SET\n");

    ht_close(dbh); 
    
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
