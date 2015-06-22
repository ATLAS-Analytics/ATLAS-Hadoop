res <- read.table( "result.csv", header=FALSE, sep=",",  col.names=c("taskid", "njobs", "taskDuration", "avgJobDuration", "avgJobWaitTime"));
njHist<-hist(log10(res$njobs));
njHist$counts = log10(njHist$counts);
plot(njHist,ylim=c(0.1,5),xlab="log(njobs)",ylab="log(freq. njobs)",main="jobs per task");

tdHist<-hist(res$taskDuration)
tdHist$counts = log10(tdHist$counts)
plot(tdHist,ylim=c(0.1,5),xlab="task duration [s]", ylab="log(njobs)",main="task durations")

jdHist<-hist(res$avgJobDuration)
jdHist$counts = log10(jdHist$counts)
plot(jdHist,ylim=c(0.1,5),ylab="log(counts)",xlab="average job duration [s]",main="average job duration")

jwHist<-hist(res$avgJobWaitTime)
jwHist$counts = log10(jwHist$counts)
plot(jwHist,ylim=c(0.1,5),ylab="log(counts)",xlab="average job waiting time [s]",main="average job waiting time")

boxplot(res)