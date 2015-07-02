res <- read.table( "nongrid.csv", header=FALSE, sep=",",  col.names=c("ReadCount", "ReadSize", "CacheSize", "AccessedFiles", "AccessedBranches", "AccessedContainers"));

pl1 <- hist(res$ReadCount,breaks=50);
pl1$counts = log10(pl1$counts);

pl2 <- hist(res$ReadSize,breaks=50);
pl2$counts = log10(pl2$counts);

pl3 <- hist(res$CacheSize,breaks=50);
pl3$counts = log10(pl3$counts);

pl4 <- hist(res$AccessedBranches,breaks=50);

par(mfrow=c(2,2))

plot (pl1, ylim=c(0.1,3),ylab="log(jobs)",main="Read Count");
plot (pl2, ylim=c(0.1,3),ylab="log(jobs)",main="Read Size");
plot (pl3, ylim=c(0.1,3),ylab="log(jobs)",main="Cache Size");
plot (pl4,ylab="jobs",main="Accessed Branches");
