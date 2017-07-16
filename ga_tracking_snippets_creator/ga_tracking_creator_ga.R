rm(list = ls())
#set your workspace, if pc
#setwd("C:/yourname/yourfolderpath/")
#set your workspace, if mac
#setwd("/Users/yourname/yourfolderpath/")
#make a csv table with your category, action, label and value as the column headers
track <- read.table("yourfile.csv", header=TRUE, sep=",")
is.data.frame(track)
#print track to see if there are any mistakes in the data frame
#print(track)
for(i in seq_len(nrow(track))){
  print(paste("analytics.trackEvent('",track$category[i],"', '",track$action[i],"', '",track$label[i],"', ",track$value[i],")",sep=''))}
