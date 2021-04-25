#! /usr/bin/env bash

echo "test"
# передаем управление файлу с командами commans.txt
for ((i = 0; i <= 10; i++))
do
./hashdbtest new < commands.txt
done 
rm new
echo "end"