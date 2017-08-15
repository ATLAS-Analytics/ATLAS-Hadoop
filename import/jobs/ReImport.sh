year=2016
month=12
ind=${year}-${month}
for d in `seq 3 4`;
    do
        day=$(printf %02d $d)
        for h in `seq 18 21`;
            do
                echo $day
                hour=$(printf %02d $h)
                echo $ind-${day}_$hour
                pig -4 log4j.properties -f toEScl.pig -param INPD=${ind}-${day}_$hour -param ININD=${ind}-${day}
            done
    done
