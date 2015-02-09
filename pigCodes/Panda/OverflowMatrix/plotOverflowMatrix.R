library(reshape2)

fails = read.csv("nJobsFailed.csv")
mfails<-acast(fails, Source~Destination, value.var="jobs")
mfails[is.na(mfails)] <- 0
#heatmap.2(mfails,dendrogram="none",trace = "none",lmat=rbind( c(0, 3, 4), c(2,1,0 ) ), lwid=c(1.5, 4, 2 ))