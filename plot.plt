set terminal wxt 0
set xrange [1 : 1000]
set title "set key time"
set xlabel "keys amount"
set ylabel "time"
set grid
plot "data.txt" using :1 with line

set terminal wxt 1
set xrange [1 : 1000]
set title "get key time"
set xlabel "keys amount"
set ylabel "time"
set grid
plot "data.txt" using :2 with line

set terminal wxt 2
set xrange [1 : 1000]
set title "del key time"
set xlabel "keys amount"
set ylabel "time"
set grid
plot "data.txt" using :3 with line