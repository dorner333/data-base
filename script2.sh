#! /usr/bin/env bash

echo "start test (hash function)"

# файл с названиями хэш-функций
file="hash_functions.txt"

# пишем команды в файл commands.txt
printf "MASS 0 100000\nGET key100000\nDEL key100000\n" > commands.txt

# извлекаем название хэш-функции
for function_name in $(cat $file)
do
# передаем управление файлу с командами
./hashdbtest new $function_name < commands.txt
# удаляем файл с базой
rm new
done

# удаляем уже ненужный файл с командами
rm commands.txt
echo "end"