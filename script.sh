#! /usr/bin/env bash

echo "test"

# отчищаем файл с данными
rm data.txt

# запускаем цикл
for ((i = 1000; i <= 160000; i+=1000))
do
# пишем команды в файл commands.txt
printf "MASS 0 $i\nGET key$i\nDEL key$i\n" > commands.txt
# передаем управление файлу с командами commans.txt
./hashdbtest new < commands.txt
done

# удаляем файл с базой
rm new
rm commands.txt
echo "end"