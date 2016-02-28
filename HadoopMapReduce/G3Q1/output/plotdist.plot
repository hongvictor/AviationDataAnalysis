set terminal png
set output "t1g3q1.png"

set title "Popularity Distribution of Airports";
set ylabel "Number of Passengers";
set xlabel "Airports (Sorted by Passengers)";
set key left top
set log y
set log x

plot "part-r-00000" using 2 title "Passengers" with linespoints


