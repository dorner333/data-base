#line 669 "hashdb.nw"
#ifndef _HASHDB_H_
#define _HASHDB_H_
#include <stdint.h>

typedef struct _DB DB;



#line 772 "hashdb.nw"
typedef struct _Stat {
    uint64_t keys;
    uint16_t tables;
    uint64_t capacity;
    uint64_t keysz;
    uint64_t valuesz;
    uint16_t maxlen;
    uint64_t nodes;
} Stat;

#line 679 "hashdb.nw"
DB* ht_open(const char* filename, size_t initial_capacity);

int ht_set(DB* dbh, const char* key, const char* value);
int ht_get(DB* dbh, const char* key, char** value);
int ht_del(DB* dbh, const char* key);

int ht_close(DB* dbh);

int ht_get_stat(DB* dbh, Stat* stat);

#endif /* _HASHDB_H_ */
