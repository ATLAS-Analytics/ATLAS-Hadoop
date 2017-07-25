year=2017
month=05
ind=${year}-${month}
for d in `seq 10 12`;
    do
        day=$(printf %02d $d)
        for h in `seq 0 23`;
            do
                echo $day
                hour=$(printf %02d $h)
                echo $ind-${day}_$hour
                ./PandaTaskSingleImporter.sh ${ind}-${day} $hour ${ind}
                #pig -4 log4j.properties -f TasksToESuc.pig -param INPD=${ind}-${day}_$hour -param ININD=${ind}
            done
    done
