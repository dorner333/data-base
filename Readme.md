# Readme
Компиляция и запус:
```
make All
```
Запуск:
```
./hashdbtest <filename>
```
Компиляция с ключем -Ofast и отображение времени компиляции:
```
make details
```
Для отображения времени работы программы запуск с ключем time:
```
time ./hashdbtest <filename>
```

# Профилирование и тесты
Запуск с профилированием
```
make details
make profile
```
Для профилироваия и визуализации результатов используем valgrind, callgrind и kcachegrind, для построения графиков используем gnuplot.
![Профилирование](https://github.com/ksartamonov/data-base/blob/stat/pictures/testing.png)
Построим график зависимости времени вставки, от количества ключей в базе. Как видно, в приближении время вставки пропорционально количеству ключей.
![График 1](https://github.com/ksartamonov/data-base/blob/stat/pictures/plot1.jpg)
Построим графики зависимости времени поиска и удаления элемента по ключу от количества ключей.
![График 2](https://github.com/ksartamonov/data-base/blob/stat/pictures/plot2.jpg)
В приближении можно сказать, что обе операции выполняются за константное время, независимо от количества ключей в таблице.
![График 3](https://github.com/ksartamonov/data-base/blob/stat/pictures/plot3.jpg)
