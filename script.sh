#! /usr/bin/env bash

echo "test"

# отчищаем файл с данными
rm data.txt

# запускаем цикл
for ((i = 1; i <= 10000; i++))
do
# пишем команды в файл commands.txt
printf "MASS 0 $i\nGET key$i\nDEL key$i\n" > commands.txt
# передаем управление файлу с командами commans.txt
./hashdbtest new < commands.txt
done

# удаляем файл с базой
rm new
rm commands
echo "end"