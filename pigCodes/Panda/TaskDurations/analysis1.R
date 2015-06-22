res <- read.table( "result.csv", header=FALSE, sep=",",  col.names=c("taskid", "njobs", "taskDuration", "avgJobDuration", "avgJobWaitTime"));
njHist<-hist(log10(res$njobs))
njHist$counts = log10(njHist$counts)
plot(njHist,ylim=c(0.1,6),xlab="log(njobs)",ylab="log(freq. njobs)",main="jobs per task")

tdHist<-hist(log10(res$taskDuration),xlab="log(task duration)")

jdHist<-hist(res$avgJobDuration)
jdHist$counts = log10(jdHist$counts)
plot(jdHist,ylim=c(0.1,6),ylab="log(counts)",xlab="average job duration [s]")