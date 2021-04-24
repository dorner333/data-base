#include "hashdb.h"

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _file_cmp_block(int fh,  const char* key, size_t ks) //побайтовое равнение блоков
{
    char buf[4096] = {};
    size_t bytes = 0;
    if ( ks == 0 ) {
        log("Keys are equal\n");
        return 1;
    }
    bytes = MIN(4096, ks);
    log("Reading %lu bytes\n", bytes);
    bytes = read(fh, buf,  MIN(4096, ks)); // Считываем из файла fh MIN(4096, ks) байтов в буфер
    log("Comparing %lu bytes of key blocks '%.4s' and '%.4s'...\n", bytes, buf, key);
    if ( memcmp(buf, key, MIN(bytes, ks)) == 0 ) {
        log("Chunks are equal\n");
        return _file_cmp_block(fh, key+ks, ks-bytes);

    }
    log("Chunks are not equal\n");
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

// Добавление блока в конец файла
int _file_append_block(int fh, const char* value, off_t* offset, size_t* size)
{
    *size = strlen(value) + 1; // Вычисляем размер значения
    *offset = lseek(fh, 0, SEEK_END);
    //*offset = ftell(fh);
    return write(fh, value, *size) != *size; // Записывает *size байт из value в файл fh
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

// Добавление элемента в конец файла
int _file_append_node(int fh, Node* node, off_t* offset)
{
    *offset = lseek(fh, 0, SEEK_END);
    //*offset = ftell(fh);
    return write(fh, node, sizeof(Node)) != sizeof(Node);
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

//Добавление таблицы в конец файла
int _file_append_table(int fh, size_t capacity, off_t* offset)
{
    THeader th = (THeader){TABLE_MAGIC, capacity, 0, 0, 0, 0}; // ! в инициализации было 5 переменных !
    char buf[4096];
    size_t bytes = capacity*sizeof(Node);
    int i = 0;
    memset(buf, 0, sizeof(buf));
    *offset = lseek(fh, 0, SEEK_END); // Находим длину файла
    //*offset = ftell(fh);
    if (write(fh, &th, sizeof(th)) != sizeof(th)) return -1; // Запись в файл fh sizeof(th) байтов из th
    for (i = 0; i < bytes/sizeof(buf); i++ ) {
        if (write(fh, buf, sizeof(buf)) != sizeof(buf)) return -2; // Тоже запись в файл
    }
    bytes = bytes % sizeof(buf);
    if ( bytes  > 0)
        if (write(fh, buf, bytes) != bytes) return -2; // И тоже запись в файл
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _file_check_magic(int fh) // Проверка магии файла
{
    char magic[] = DB_MAGIC;
    char buf[4];
    lseek(fh, 0, SEEK_SET);

    if (read(fh, buf, 4) != 4 ) // Читаем первые 4 байта из файла в buf
    {
        error("Cannot read magic\n");
        return 0;
    }

    if ( memcmp(magic, buf, 4) )  // Сранивает по 4 байта из magic и из buf
    {
        error("Wrong magic\n");
        return 0;
    }
    return 1;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _file_write_header(int fh, size_t initial_capacity) // Запись названия файла
{
    Stat stat = {0}; // Обнуляем статистику
    stat.capacity = initial_capacity;
    stat.tables = 1; // делаем первую таблицу
    DHeader dh = (DHeader){DB_MAGIC, CURRENT_VERSION, stat};  //  Инициализируем заголовок базы данных
    lseek(fh, 0, SEEK_SET);
    if ( write(fh, &dh, sizeof(dh)) != sizeof(dh) ) return -1; // Запись в файл fh sizeof(dh) байт из файла &dh
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _file_load_stat(int fh, Stat* stat)
{
    lseek(fh, offsetof(DHeader, stat), SEEK_SET); //  Передвигаем указатель в файле fh на положение offsetof(DHeader, stat) во всем файле
    return read(fh, stat, sizeof(Stat)) != sizeof(Stat); // Читает sizeof(Stat) байтов из stat. Read возвращает количнество считанных байт.
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
// static - функция видна только этому объектному файлу
// inline - компилятор подставляет тело функции вместо ее вызова
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

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

// #include <openssl/md5.h>
/*
static inline uint64_t md5hash(const char* v) {
    unsigned char result[MD5_DIGEST_LENGTH];
    uint64_t* k = (uint64_t*)result;
    MD5(v, strlen(v), result);
    return *k;
}

*/

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

typedef struct _Cursor { // Для поиска элемента, сохраняет значение поиска
    int fh;
    Stat* stat;
    THeader th; // Текущая таблица
    off_t tableoff; // Смещение заголовка внутри файла
    uint64_t hash;
    uint64_t hash2;
    int idx; // индекс текущего элемента в таблице
    Node node; // текущий элемент
    off_t nodeoff; // смещение текущего элеменьа
    Node chain; // текущий элемент цепочки
    off_t chainoff; // Смещение текущего элемента внутри файла
    Node prev; // Предыдущий прочитанный элемент
    off_t prevoff; // Смещение предыдущего прочитанного элеменат
    int len; // текущая длина цепочки
}Cursor;
 // node    chain
 // v        v
 // N1->N3->N4->N5
 // N2
 // N5->N6->N7->N8 <<< cursor
 // x


//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_load_node(Cursor* cur) // Загрузка элемента таблицы из файла
{
    cur->len = 0; //текущую начальную считанную длину обнуляем
    lseek(cur->fh, cur->nodeoff, SEEK_SET); // Сместим каретку по положению cur->nodeoff отсчитывая от текущего смещения SEEK_SET
    if (read(cur->fh, &cur->node, sizeof(Node)) != sizeof(Node) )  // Если произошла ошибка с чтением
    {
        error("Cannot read node at %ld\n", cur->nodeoff);
        return 1;
    }
    log("Loaded node at %d: keyoff=%ld, keysz=%lu, valueoff=%ld, valuesz=%lu, next=%ld\n",
        cur->idx, cur->node.keyoff, cur->node.keysz,
        cur->node.valueoff, cur->node.valuesz, cur->node.next);
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_read_chain(Cursor* cur, off_t offset) { // Чтение элемента цепочки
    cur->prev = cur->chain;
    cur->prevoff = cur->chainoff;
    cur->chainoff = offset;
    lseek(cur->fh, cur->chainoff, SEEK_SET);
    if( read(cur->fh, &cur->chain, sizeof(Node)) != sizeof(Node) )  //Если произошла ошибка с чтением
    {
        error("Cannot read chain at %ld\n", cur->chainoff);
        return 1;
    }
    cur->len++; // увеличиваем длину цепочки, так как прочли один элемент
    log("Loaded node at %d: keyoff=%ld, keysz=%lu, valueoff=%ld, valuesz=%lu, next=%ld\n",
        cur->idx, cur->chain.keyoff, cur->chain.keysz,
        cur->chain.valueoff, cur->chain.valuesz, cur->chain.next);
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_read_table(Cursor* cur, off_t offset) // Чтение таблицы
{
    cur->tableoff = offset;
    lseek(cur->fh, cur->tableoff, SEEK_SET); // сместим каретку на tableoff начиная от SEEK_SET
    return read(cur->fh, &cur->th, sizeof(THeader)) != sizeof(THeader); // Считываем заголовок таблицы
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_update_node(Cursor* cur) // Запись элемента таблицы
{
    lseek(cur->fh, cur->nodeoff, SEEK_SET);  //  сместим каретку на текущее смещение элемента начиная с SEEK_SET
    return write(cur->fh, &cur->node, sizeof(Node)) != sizeof(Node); // Запишем новое значение
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_update_chain(Cursor* cur) // Запись элемента в цепочку
{
    lseek(cur->fh, cur->chainoff, SEEK_SET); // Сместим каретку на текущее смещение (именно узла элемента)
    return write(cur->fh, &cur->chain, sizeof(Node)) != sizeof(Node); // Запишем новое значение
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_update_prev(Cursor* cur) // Записать предыдущий считанный элемент, так как мы обновили текуший
{
    lseek(cur->fh, cur->prevoff, SEEK_SET);  // Сместим каретку в его положение
    return write(cur->fh, &cur->prev, sizeof(Node)) != sizeof(Node); // Запишем
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_update_table(Cursor* cur) // Запись таблицы
{
    if ( cur->len > cur->th.len ) cur->th.len = cur->len; // Переходим
    lseek(cur->fh, cur->tableoff, SEEK_SET);
    return write(cur->fh, &cur->th, sizeof(THeader)) != sizeof(THeader);
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_update_stat(Cursor* cur)  // ЗАпись статистики
{
    lseek(cur->fh, offsetof(DHeader, stat), SEEK_SET);
    return write(cur->fh, cur->stat, sizeof(Stat)) != sizeof(Stat);
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_cmp_chain(Cursor* cur, const char* key) // Сравнение ключа
{
    size_t ks = strlen(key) + 1; // Размер ключа
    log("Comparing key '%s' with cursor\n", key);
    if ( cur->chain.keysz != ks ) return 0; // Вернем 0 если размеры не равны
    log("Sizes %d are equal\n", ks);
    lseek(cur->fh, cur->chain.keyoff, SEEK_SET);
    return _file_cmp_block(cur->fh, key, ks);
}


//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_find(Cursor* cur, const char* key)  // рекурсивный проход по цепочке
{
    if ( cur->chain.valueoff && _cur_cmp_chain(cur, key) ) return 1; // Рекурсия прекрашается когда нашли элемент, лежащий по ключу
    if ( cur->chain.next ) // Иначе идем ишем дальше
    {
        log("Reading next node at %ld\n", cur->chain.next);
        _cur_read_chain(cur, cur->chain.next); //Читаем цепочку
        return _cur_find(cur, key);
    }
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_search(Cursor* cur, const char* key) // Поиск элемента по ключу
{
    cur->idx = cur->hash % cur->th.capacity; // По значению хэш функции определяем индекс в таблице
    log("Searching for key %s at table %ld idx %u\n", key, cur->tableoff, cur->idx);
    cur->nodeoff = cur->tableoff + sizeof(THeader) + sizeof(Node)*cur->idx;
    // Смещение самого элемента это смещение текущей таблицы + название + количесто элементов до этого
    _cur_load_node(cur); // Загрузим этот элемент таблицы в структуру курсора(считаем)
    cur->chainoff = cur->nodeoff; // Сначала смешение по цепочке равно смещению самого элемента
    cur->chain = cur->node; // см. строку выше
    if(_cur_find(cur, key)) //Если при проходе по цепочке нашли возвращаем 1
        return 1;
  //  cur->idx = cur->hash2 % cur->th.capacity;
    cur->nodeoff = cur->tableoff + sizeof(THeader) + sizeof(Node)*cur->idx;
    // Смещение самого элемента это смещение текущей таблицы + название + количесто элементов до этого
    _cur_load_node(cur);
    cur->chainoff = cur->nodeoff;
    cur->chain = cur->node;
    if(_cur_find(cur, key))
        return 1;
    // ! QUESTION ! Делаем это дважды
    if ( cur->th.next ) // Если есть следуюшая табличка, то в ней тоже ищем
    {
        _cur_read_table(cur, cur->th.next);
        return _cur_search(cur, key);
    }
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_write_chain(Cursor* cur, const char* key, const char* v) // Запись в цепочку
{
    Node new = {0};
    _file_append_block(cur->fh, key, &new.keyoff, &new.keysz); // Добавление нового блока для размера ключа
    _file_append_block(cur->fh, v, &new.valueoff, &new.valuesz); // Добавление нового блока для размера значения
    _file_append_node(cur->fh, &new, &cur->chain.next); // Добавление самого элемента
    _cur_update_chain(cur); // Добавление цепочки (самого элемента)
    cur->th.size ++; // Так как записали размер увеличился
    cur->len++;
    _cur_update_table(cur); // Добавление самой таблицы
    cur->stat->keysz += new.keysz; // Статистику тоже обновляем, увеличим на размер этого ключа размер ключей
    cur->stat->valuesz += new.valuesz; // Аналогично со значениями
    cur->stat->keys ++; // Количество ключей тоже увеличилось
    if (cur->stat->maxlen < cur->len) cur->stat->maxlen = cur->len; // Если превысили максимальную
    _cur_update_stat(cur); // Запись статистики
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
// ! QUESTION:   _cur_update_node  делает то же, что и _file_append_block  !
//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_write_node(Cursor* cur, const char* key, const char* v)  // (см. функцию выше) Запись в таблицу пары ключ-значение
{
    _file_append_block(cur->fh, key, &cur->node.keyoff, &cur->node.keysz); // Добавление блока для ключа и запись
    _file_append_block(cur->fh, v, &cur->node.valueoff, &cur->node.valuesz);  // Добавление блока для значения и запись
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

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_set_value(Cursor* cur, const char* value) // Присваивает value какое-то новое значение  (обноление элемента)
{
    size_t oldsz = cur->chain.valuesz;
    _file_append_block(cur->fh, value, &cur->chain.valueoff, &cur->chain.valuesz); // Записывает
    _cur_update_chain(cur); // Запись в цепочку
    cur->stat->valuesz += cur->chain.valuesz - oldsz; // Размер тоже изменился
    _cur_update_stat(cur); // Запись статистики
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _cur_del_node(Cursor* cur) // Удаление элемента (либо из цепочки лио из самой таблицы, так как если курсоро на элементе,
  // то совпадают chainoff, valueoff
{
    if ( cur->nodeoff != cur->chainoff  ) // Если удаляем из цепочки
    {
        cur->prev.next = cur->chain.next;
        _cur_update_prev(cur);
    }

    else // Если удаляем сам элемент
    {
        cur->node.valueoff = 0;
        _cur_update_node(cur);
    }
    cur->th.size --;
    _cur_update_table(cur);
    cur->stat->valuesz -= cur->node.valuesz;
    cur->stat->keysz -= cur->node.keysz;
    _cur_update_stat(cur);
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int _ht_search(DB* db, Cursor* cur, const char* key) // Инициализация курсора
{
    memset(cur, 0, sizeof(Cursor)); // Обнуление курсора
    cur->stat = &db->stat;
    cur->fh = db->fh;
   cur->hash = db->hash(key);
  //  cur->hash2 = db->hash2(key);
    _cur_read_table(cur, (off_t)sizeof(DHeader)); // Считываем саму табличку
    return _cur_search(cur, key); // Ищем с помощью курсора
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

DB* ht_open(const char* filename, size_t initial_capacity)  // Открытие файла базы данных
{
    DB* dbh;
    FILE* f = fopen(filename, "a+t");
    if ( f )
    {
        if (_file_check_magic(fileno(f))) {
            f = freopen(NULL, "r+", f);
        } else {
            error("Wrong magic file %s\n", filename);
            return NULL;
        }
    }

    else
    {
        off_t first_table_offset;
        f = fopen(filename, "w+");
        if ( !f ) {
            error("Cannot open file %s for writing\n", filename);
            return NULL;
        }
        _file_write_header(fileno(f), initial_capacity);
        _file_append_table(fileno(f), initial_capacity, &first_table_offset);
    }
    if ( !f )
    {
        error("Cannot open file %s\n", filename);
        return NULL;
    }
    dbh = malloc(sizeof(DB));
    dbh->fh = fileno(f);
    //dbh->hash = rot1333;
    dbh->hash = rot1333; // ! QUESTION !
    //dbh->hash2 = rot1333;
    _file_load_stat(dbh->fh, &dbh->stat);
    return dbh;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int ht_close(DB* db) // Закрытие файла базы данных
{
    if( db ) {
        if (db->fh) close(db->fh);
        free(db);
    }
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int ht_set(DB* db, const char* key, const char* value)  //  Установка ассоциации (присваиваем по ключу значение)
{
    Cursor cur;
    if ( _ht_search(db, &cur, key) )
        // элемент с нужным ключом найден -- обновляем значение
        return _cur_set_value(&cur, value);
    else {
        // элемент не найден
        if ( cur.th.nodes*100 >= cur.th.capacity*56)
        {
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

        if ( cur.nodeoff == cur.chainoff && !cur.node.keyoff )
            return _cur_write_node(&cur, key, value);
        else
            return _cur_write_chain(&cur, key, value);
    }
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int ht_get(DB* db, const char* key, char** value) // Получение значения по ключу
{
    Cursor cur;
    if ( _ht_search(db, &cur, key) )
    {
        *value = malloc(cur.chain.valuesz);
        lseek(cur.fh, cur.chain.valueoff, SEEK_SET);
        if ( read(cur.fh, *value, cur.chain.valuesz) == cur.chain.valuesz )
        {
            return 1;
        }
        else
        {
            error("Cannod read %lu bytes for value at %ld\n", cur.chain.valuesz,
                cur.chain.valueoff);
            return 0;
        }
    }
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int ht_del(DB* db, const char* key) // Удаление значения по ключу
{
    Cursor cur;
    if ( _ht_search(db, &cur, key) )
    {
        _cur_del_node(&cur);
        return 1;
    }
    return 0;
}

//-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

int ht_get_stat(DB* dbh, Stat* stat)
{
    *stat = dbh->stat;
    return 0;
}
