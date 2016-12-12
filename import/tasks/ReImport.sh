year=2016
month=10
ind=${year}-${month}
for d in `seq 1 22`;
    do
        day=$(printf %02d $d)
        for h in `seq 0 23`;
            do
                echo $day
                hour=$(printf %02d $h)
                echo $ind-${day}_$hour
                pig -4 log4j.properties -f toEScl.pig -param INPD=${ind}-${day}_$hour -param ININD=${ind}
            done
    done
