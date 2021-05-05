#! /usr/bin/env bash

echo "start test (key length)"

rm data.txt

# запускаем цикл
for ((i = 1; i <= 20; i++))
do
# пишем команды в файл commands.txt
printf "MASS 0 $i\nSTAT\nGET key$i\nDEL key$i\n" > commands.txt
# передаем управление файлу с командами commans.txt
./hashdbtest new < commands.txt
# удаляем файл с базой
rm new
done

# удаляем уже ненужный файл с командами
rm commands.txt

echo "end"