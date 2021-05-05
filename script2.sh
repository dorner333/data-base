#! /usr/bin/env bash

echo "start test (hash function)"

# файл с названиями хэш-функций
file  = "hash_funtions.txt"

# извлекаем название хэш-функции
for function_name in $(cat $file)
do
# передаем управление файлу с командами
./hashdbtest new $function_name > hf_test.txt
# удаляем файл с базой
rm new
done

echo "end"