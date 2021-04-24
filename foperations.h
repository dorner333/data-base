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
