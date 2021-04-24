#ifndef _HASHDB_H_
#define _HASHDB_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>

#include <time.h>

#define error(str, ...) fprintf(stderr, "[err] " str, ## __VA_ARGS__)
#define log(str, ...)  {}

#define MIN(a, b) ((a)<(b)? (a) : (b))


#define DB_MAGIC  {'H', 'T', 'd', 'b'}
#define TABLE_MAGIC  {'H', 'T', 'T', 'b'}
#define CURRENT_VERSION 1

//Структуры для сохранения определенного времени
struct timespec mt1, mt2;

typedef struct _Stat {
    uint64_t keys; // Количество ключей
    uint16_t tables; // Количество таблиц
    uint64_t capacity; // емкость бд
    uint64_t keysz; // Размер ключа
    uint64_t valuesz; // Рарзмер значения
    uint16_t maxlen; // Максимальная длина
    uint64_t nodes; // Количество элементов (узлов)
} Stat;

typedef struct _DB {
    int fh; // сам файл
    uint64_t (*hash)(const char*);
    uint64_t (*hash2)(const char*);
    Stat stat; // Статистика таблицы
} DB;


typedef struct _DHeader { // Заголовок базы данных
    char magic[4]; // Магическая последовательность - информация о бд
    uint32_t version; // Текущая версия (если формат бд будет менятьсф)
    struct _Stat stat; // Статистика (определена в хедере)
} DHeader;

typedef struct _THeader { // Заголовок текущей таблицы
    char magic[4]; /// Информация о таблице
    uint32_t capacity; //Общая емкость таблицы
    uint32_t size; // Текущий разер таблицы
    uint32_t nodes; // Количество узлов
    uint16_t len; //
    int64_t next; // Указатель на следующую таблицу
} THeader;

// Если элемент таблицы пустой то keyoff = 0
typedef struct _Node { // Структура самого элемента таблицы
    int64_t keyoff; // Смещегние ключа
    uint64_t keysz; // Размер ключа
    int64_t valueoff; //смещение значения
    uint64_t valuesz; // размер значения
    int64_t next; // указатель на следующий элемент
} Node;

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

DB* ht_open(const char* filename, size_t initial_capacity);
int ht_set(DB* dbh, const char* key, const char* value);
int ht_get(DB* dbh, const char* key, char** value);
int ht_del(DB* dbh, const char* key);
int ht_close(DB* dbh);
int ht_get_stat(DB* dbh, Stat* stat);

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

#endif /* _HASHDB_H_ */
