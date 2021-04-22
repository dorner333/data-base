#line 786 "hashdb.nw"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include "hashdb.h"



#define error(str, ...) fprintf(stderr, "[err] " str, ## __VA_ARGS__)
#define log(str, ...)  {}
//fprintf(stderr, "[log] " str, ##  __VA_ARGS__)

#define MIN(a, b) ((a)<(b)? (a) : (b))

#line 662 "hashdb.nw"
#define DB_MAGIC  {'H', 'T', 'd', 'b'}
#define TABLE_MAGIC  {'H', 'T', 'T', 'b'}
#define CURRENT_VERSION 1

#line 540 "hashdb.nw"

struct _DB {
    int fh; // Fie header
    uint64_t (*hash)(const char*);
    uint64_t (*hash2)(const char*);
    Stat stat;
};

#line 38 "hashdb.nw"

typedef struct _DHeader {
    char magic[4];
    uint32_t version;
    struct _Stat stat;
} DHeader;

#line 57 "hashdb.nw"

typedef struct _THeader {
    char magic[4];
    uint32_t capacity;
    uint32_t size;
    uint32_t nodes;
    uint16_t len;
    int64_t next;
} THeader;

#line 73 "hashdb.nw"

typedef struct _Node {
    int64_t keyoff;
    uint64_t keysz;
    int64_t valueoff;
    uint64_t valuesz;
    int64_t next;
} Node;




#line 695 "hashdb.nw"
static inline uint32_t murmur_32_scramble(uint32_t k) {
    k *= 0xcc9e2d51;
    k =   (k >> 17)| (k << 15);
    k *= 0x1b873593;
    return k;
}
static inline uint32_t murmur3_32(const uint8_t* key, size_t len, uint32_t seed)
{
	uint32_t h = seed;
    uint32_t k;
    /* Read in groups of 4. */
    for (size_t i = len >> 2; i; i--) {
        // Here is a source of differing results across endiannesses.
        // A swap here has no effects on hash properties though.
        memcpy(&k, key, sizeof(uint32_t));
        key += sizeof(uint32_t);
        h ^= murmur_32_scramble(k);
        h = (h >> 19) | (h << 13)  ;
        h = h * 5 + 0xe6546b64;
    }
    /* Read the rest. */
    k = 0;
    for (size_t i = len & 3; i; i--) {
        k <<= 8;
        k |= key[i - 1];
    }
    // A swap is *not* necessary here because the preceding loop already
    // places the low bytes in the low places according to whatever endianness
    // we use. Swaps only apply when the memory is copied in a chunk.
    h ^= murmur_32_scramble(k);
    /* Finalize. */
	h ^= len;
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;
	return h;
}

static inline uint64_t rot1333(const uint8_t* v) {
    //return murmur3_32(v, strlen(v), 0x989124f1);
    uint64_t h = 0xb928c940def2313c;
    uint8_t p = 241;
    uint8_t pp = 117;
    while(*v) {
        //h = (h >> 31 ) | (h << 33);
        h += 0xaa4294967291*(17*(*v)*p*pp+31*(*v)*p + 5*(*v)+3*p + pp + 11*(*v)) ; // + 0x55555555555559*(((*v) >> 5) | ((*v) << 3));
        //h ^= (h <<32);
        h = (h >> 51) | (h << 13);
        h *= 128000115967;
        h = (h >> 45) | (h << 19);
        h ^= (h >> 31) ^ (h << 33);
        h = (h >> 49) | (h << 15);
        h = (h >> 47) | (h << 17);
        pp = p;
        p = *v;
        v++;

    }
    return h;
}

// #include <openssl/md5.h>
/*
static inline uint64_t md5hash(const char* v) {
    unsigned char result[MD5_DIGEST_LENGTH];
    uint64_t* k = (uint64_t*)result;
    MD5(v, strlen(v), result);
    return *k;
}

*/
#line 130 "hashdb.nw"
typedef struct _Cursor {
    int fh;
    Stat* stat;
    THeader th;
    off_t tableoff;
    uint64_t hash;
    uint64_t hash2;
    int idx;
    Node node;
    off_t nodeoff;
    Node chain;
    off_t chainoff;
    Node prev;
    off_t prevoff;
    int len;
}Cursor;
 // node    chain
 // v        v
 // N1->N3->N4->N5
 // N2
 // N5->N6->N7->N8 <<< cursor


#line 355 "hashdb.nw"
int _file_cmp_block(int fh,  const char* key, size_t ks) {
    char buf[4096];
    size_t bytes;
    if ( ks == 0 ) {
        log("Keys are equal\n");
        return 1;
    }
    bytes = MIN(4096, ks);
    log("Reading %lu bytes\n", bytes);
    bytes = read(fh, buf,  MIN(4096, ks));
    log("Comparing %lu bytes of key blocks '%.4s' and '%.4s'...\n", bytes, buf, key);
    if ( memcmp(buf, key, MIN(bytes, ks)) == 0 ) {
        log("Chunks are equal\n");
        return _file_cmp_block(fh, key+ks, ks-bytes);

    }
    log("Chunks are not equal\n");
    return 0;
}
#line 385 "hashdb.nw"
int _file_append_block(int fh, const char* value, off_t* offset, size_t* size) { //Добавление блока в конец файла
    *size = strlen(value) + 1;
    *offset = lseek(fh, 0, SEEK_END);
    //*offset = ftell(fh);
    return write(fh, value, *size) != *size;
}

#line 393 "hashdb.nw"
int _file_append_node(int fh, Node* node, off_t* offset) {  //Добавление элемента в конец файла
    *offset = lseek(fh, 0, SEEK_END);
    //*offset = ftell(fh);
    return write(fh, node, sizeof(Node)) != sizeof(Node);
}

#line 400 "hashdb.nw"
int _file_append_table(int fh, size_t capacity, off_t* offset) { // Добавление таблицы
    THeader th = (THeader){TABLE_MAGIC, capacity, 0, 0, 0};
    char buf[4096];
    size_t bytes = capacity*sizeof(Node);
    int i;
    memset(buf, 0, sizeof(buf));
    *offset = lseek(fh, 0, SEEK_END);
    //*offset = ftell(fh);
    if (write(fh, &th, sizeof(th)) != sizeof(th)) return -1;
    for (i = 0; i < bytes/sizeof(buf); i++ ) {
        if (write(fh, buf, sizeof(buf)) != sizeof(buf)) return -2;
    }
    bytes = bytes % sizeof(buf);
    if ( bytes  > 0)
        if (write(fh, buf, bytes) != bytes) return -2;
    return 0;
}

#line 567 "hashdb.nw"

int _file_check_magic(int fh) { // Просто проверка магии
    char magic[] = DB_MAGIC;
    char buf[4];
    lseek(fh, 0, SEEK_SET);
    if (read(fh, buf, 4) != 4 ) {
        error("Cannot read magic\n");
        return 0;
    }
    if ( memcmp(magic, buf, 4) ) {
        error("Wrong magic\n");
        return 0;
    }
    return 1;
}

#line 584 "hashdb.nw"

int _file_write_header(int fh, size_t initial_capacity) {
    Stat stat = {0};
    stat.capacity = initial_capacity;
    stat.tables = 1;
    DHeader dh = (DHeader){DB_MAGIC, CURRENT_VERSION, stat};
    lseek(fh, 0, SEEK_SET);
    if ( write(fh, &dh, sizeof(dh)) != sizeof(dh) ) return -1;
    return 0;
}

#line 596 "hashdb.nw"
int _file_load_stat(int fh, Stat* stat) {
    lseek(fh, offsetof(DHeader, stat), SEEK_SET);
    return read(fh, stat, sizeof(Stat)) != sizeof(Stat);
}

#line 314 "hashdb.nw"
int _cur_load_node(Cursor* cur) { //Загрузка элемента таблицы по курсору
    cur->len = 0;
    lseek(cur->fh, cur->nodeoff, SEEK_SET);
    if (read(cur->fh, &cur->node, sizeof(Node)) != sizeof(Node) ) {
        error("Cannot read node at %ld\n", cur->nodeoff);
        return 1;
    }
    log("Loaded node at %d: keyoff=%ld, keysz=%lu, valueoff=%ld, valuesz=%lu, next=%ld\n",
        cur->idx, cur->node.keyoff, cur->node.keysz,
        cur->node.valueoff, cur->node.valuesz, cur->node.next);
    return 0;

}

#line 330 "hashdb.nw"
int _cur_read_chain(Cursor* cur, off_t offset) {
    cur->prev = cur->chain;
    cur->prevoff = cur->chainoff;
    cur->chainoff = offset;
    lseek(cur->fh, cur->chainoff, SEEK_SET);
    if( read(cur->fh, &cur->chain, sizeof(Node)) != sizeof(Node) ) {
        error("Cannot read chain at %ld\n", cur->chainoff);
        return 1;
    }
    cur->len++;
    log("Loaded node at %d: keyoff=%ld, keysz=%lu, valueoff=%ld, valuesz=%lu, next=%ld\n",
        cur->idx, cur->chain.keyoff, cur->chain.keysz,
        cur->chain.valueoff, cur->chain.valuesz, cur->chain.next);
}

#line 347 "hashdb.nw"
int _cur_read_table(Cursor* cur, off_t offset) {
    cur->tableoff = offset;
    lseek(cur->fh, cur->tableoff, SEEK_SET);
    return read(cur->fh, &cur->th, sizeof(THeader)) != sizeof(THeader);
}

#line 623 "hashdb.nw"
int _cur_update_node(Cursor* cur) {
    lseek(cur->fh, cur->nodeoff, SEEK_SET);
    return write(cur->fh, &cur->node, sizeof(Node)) != sizeof(Node);

}

#line 638 "hashdb.nw"
int _cur_update_chain(Cursor* cur) {
    lseek(cur->fh, cur->chainoff, SEEK_SET);
    return write(cur->fh, &cur->chain, sizeof(Node)) != sizeof(Node);
}
#line 631 "hashdb.nw"
int _cur_update_prev(Cursor* cur) {
    lseek(cur->fh, cur->prevoff, SEEK_SET);
    return write(cur->fh, &cur->prev, sizeof(Node)) != sizeof(Node);
}
#line 645 "hashdb.nw"
int _cur_update_table(Cursor* cur) { // Обновление заголовка таблицы после внесения изменений, связанных со статистикой
    if ( cur->len > cur->th.len ) cur->th.len = cur->len;
    lseek(cur->fh, cur->tableoff, SEEK_SET);
    return write(cur->fh, &cur->th, sizeof(THeader)) != sizeof(THeader);
}
#line 653 "hashdb.nw"
int _cur_update_stat(Cursor* cur) { // Обновление структуры -- статистики всей базы данных
    lseek(cur->fh, offsetof(DHeader, stat), SEEK_SET);
    return write(cur->fh, cur->stat, sizeof(Stat)) != sizeof(Stat);
}
#line 302 "hashdb.nw"
int _cur_cmp_chain(Cursor* cur, const char* key) { // Побайтовое сравнение ключа
    size_t ks = strlen(key) + 1;
    log("Comparing key '%s' with cursor\n", key);
    if ( cur->chain.keysz != ks ) return 0;
    log("Sizes %d are equal\n", ks);
    lseek(cur->fh, cur->chain.keyoff, SEEK_SET);
    return _file_cmp_block(cur->fh, key, ks);
}

#line 250 "hashdb.nw"
int _cur_find(Cursor* cur, const char* key) { // Проход по цепочке
    if ( cur->chain.valueoff && _cur_cmp_chain(cur, key) ) return 1;
    if ( cur->chain.next ) {
        log("Reading next node at %ld\n", cur->chain.next);
        _cur_read_chain(cur, cur->chain.next);
        return _cur_find(cur, key);
    }
    return 0;
}

#line 264 "hashdb.nw"
int _cur_search(Cursor* cur, const char* key) { // Либо ищем по ключу, либо ищем место для записи
    cur->idx = cur->hash % cur->th.capacity; // индекс присвавиваем остатку хэша

    log("Searching for key %s at table %ld idx %u\n", key, cur->tableoff, cur->idx);

    cur->nodeoff = cur->tableoff + sizeof(THeader) + sizeof(Node)*cur->idx;
    _cur_load_node(cur);
    cur->chainoff = cur->nodeoff;
    cur->chain = cur->node;
    if(_cur_find(cur, key))
        return 1;
  //  cur->idx = cur->hash2 % cur->th.capacity;
    // cur->nodeoff = cur->tableoff + sizeof(THeader) + sizeof(Node)*cur->idx;
    // _cur_load_node(cur);
    // cur->chainoff = cur->nodeoff;
    // cur->chain = cur->node;
    // if(_cur_find(cur, key))
    //     return 1;


    // if (cur->node->keyoff == 0)
    // {
    //
    // }

    if ( cur->th.next ) {
        _cur_read_table(cur, cur->th.next);
        return _cur_search(cur, key);
    }
    return 0;

}

// #line 160 "hashdb.nw"
// int _cur_write_chain(Cursor* cur, const char* key, const char* v) { //Дописывание в конец цепочки элементов новго элемента и пары ключ-значение
//     Node new = {0};
//     _file_append_block(cur->fh, key, &new.keyoff, &new.keysz);
//     _file_append_block(cur->fh, v, &new.valueoff, &new.valuesz);
//     _file_append_node(cur->fh, &new, &cur->chain.next);
//     _cur_update_chain(cur);
//     cur->th.size ++;
//     cur->len++;
//     _cur_update_table(cur);
//     cur->stat->keysz += new.keysz;
//     cur->stat->valuesz += new.valuesz;
//     cur->stat->keys ++;
//     if (cur->stat->maxlen < cur->len) cur->stat->maxlen = cur->len;
//     _cur_update_stat(cur);
//     return 0;
// }

#line 187 "hashdb.nw"
int _cur_write_node(Cursor* cur, const char* key, const char* v) {
    _file_append_block(cur->fh, key, &cur->node.keyoff, &cur->node.keysz);
    _file_append_block(cur->fh, v, &cur->node.valueoff, &cur->node.valuesz);
    _cur_update_node(cur);
    cur->th.size ++;
    cur->th.nodes ++;
    _cur_update_table(cur);
    cur->stat->keysz += cur->node.keysz;
    cur->stat->valuesz += cur->node.valuesz;
    cur->stat->keys ++;
    cur->stat->nodes ++;
    _cur_update_stat(cur);
    return 0;
}
#line 208 "hashdb.nw"
int _cur_set_value(Cursor* cur, const char* value) {  // Обновление элемента по ключу
    size_t oldsz = cur->chain.valuesz;
    _file_append_block(cur->fh, value, &cur->chain.valueoff, &cur->chain.valuesz);
    _cur_update_chain(cur);
    cur->stat->valuesz += cur->chain.valuesz - oldsz;
    _cur_update_stat(cur);
}
#line 223 "hashdb.nw"
int _cur_del_node(Cursor* cur) { // Удаление элемента таблицы
    if ( cur->nodeoff != cur->chainoff  ) { // nodeoff и chainoff должны совпадать
        cur->prev.next = cur->chain.next;
        _cur_update_prev(cur);
    } else {
        cur->node.valueoff = 0; // Если совпадают, то смещение valueoff сделаем равным 0
        _cur_update_node(cur);
    }
    cur->th.size --;
    _cur_update_table(cur);
    cur->stat->valuesz -= cur->node.valuesz;
    cur->stat->keysz -= cur->node.keysz;
    _cur_update_stat(cur);
}

// #line 289 "hashdb.nw"
int _ht_search(DB* db, Cursor* cur, const char* key) {  // Инициализация курсора
    memset(cur, 0, sizeof(Cursor));
    cur->stat = &db->stat;
    cur->fh = db->fh;
   cur->hash = db->hash(key);
  //  cur->hash2 = db->hash2(key);
    _cur_read_table(cur, (off_t)sizeof(DHeader));
    return _cur_search(cur, key);
}

#line 497 "hashdb.nw"
DB* ht_open(const char* filename, size_t initial_capacity) { // Открытие хэщ-таблицы
    DB* dbh;
    FILE* f = fopen(filename, "r");
    if ( f ) {
        if (_file_check_magic(fileno(f))) {
            f = freopen(NULL, "r+", f);
        } else {
            error("Wrong magic file %s\n", filename);
            return NULL;
        }
    } else {
        off_t first_table_offset;
        f = fopen(filename, "w+");
        if ( !f ) {
            error("Cannot open file %s for writing\n", filename);
            return NULL;
        }
        _file_write_header(fileno(f), initial_capacity);
        _file_append_table(fileno(f), initial_capacity, &first_table_offset);
    }
    if ( !f ) {
        error("Cannot open file %s\n", filename);
        return NULL;
    }
    dbh = malloc(sizeof(DB));
    dbh->fh = fileno(f);
    //dbh->hash = rot1333;
    dbh->hash = rot1333;
    //dbh->hash2 = rot1333;
    _file_load_stat(dbh->fh, &dbh->stat);
    return dbh;
}
#line 549 "hashdb.nw"
int ht_close(DB* db) { // Закрытие хэш-таблицы
    if( db ) {
        if (db->fh) close(db->fh);
        free(db);
    }
    return 0;
}


#line 436 "hashdb.nw"
int ht_set(DB* db, const char* key, const char* value) { // Помещение пары ключ-значение в базу данных
     Cursor cur;
    if ( _ht_search(db, &cur, key) )
        // элемент с нужным ключом найден -- обновляем значение
        return _cur_set_value(&cur, value);
    else {
        // элемент не найден
        if ( cur.th.nodes*100 >= cur.th.capacity*67) {
            // создаем новую таблицу, если текущая таблица переполнена
            error("Inserting table of size %u... when size=%u, nodes=%u, maxlen=%u ", cur.th.capacity*2+1, cur.th.size, cur.th.nodes, cur.th.len);
            _file_append_table(cur.fh, cur.th.capacity*2+1, &cur.th.next);
            cur.stat->tables ++;
            cur.stat->capacity += cur.th.capacity*2+1;
            _cur_update_stat(&cur);
            _cur_update_table(&cur);
            _cur_read_table(&cur, cur.th.next);
            _cur_search(&cur, key);
            error("DONE\n");
        }
        if (!cur.node.keyoff )
            return _cur_write_node(&cur, key, value);
        else // ТУТ ДОБАВИТЬ ОТКРЫТУЮ АДРЕСАЦИЮ !!!
          {
            _cur_search(&cur, key);
            _cur_write_node(&cur, key, value);
          }
    }
}

#line 464 "hashdb.nw"
int ht_get(DB* db, const char* key, char** value) { // Получение значения по ключу
    Cursor cur;
    if ( _ht_search(db, &cur, key) ) {
        *value = malloc(cur.chain.valuesz);
        lseek(cur.fh, cur.chain.valueoff, SEEK_SET);
        if ( read(cur.fh, *value, cur.chain.valuesz) == cur.chain.valuesz ) {
            return 1;
        } else {
            error("Cannot read %lu bytes for value at %ld\n", cur.chain.valuesz,
                cur.chain.valueoff);
            return 0;
        }
    }
    return 0;
}

#line 482 "hashdb.nw"
int ht_del(DB* db, const char* key) { //Удаление значения по ключу
    Cursor cur;
    if ( _ht_search(db, &cur, key) ) {
        _cur_del_node(&cur);
        return 1;
    }
    return 0;
}
#line 532 "hashdb.nw"
int ht_get_stat(DB* dbh, Stat* stat) { //
    *stat = dbh->stat;
    return 0;
}
