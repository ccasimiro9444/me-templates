rm(list = ls())
#set your workspace, if pc
#setwd("C:/yourname/yourfolderpath/")
#set your workspace, if mac
#setwd("/Users/yourname/yourfolderpath/")
#make a csv table with your (click) id, (GA) category, action, label, value and (function) running number as the column headers
track <- read.table("yourfile.csv", header=TRUE, sep=",")
is.data.frame(track)
#print track to see if there are any mistakes in the data frame
#print(track)
for(i in seq_len(nrow(track))){
  print(paste("function segment",track$runningnumber[i],"() {analytics.track('",track$action[i],"', {category: '",track$category[i],"',label: '",track$label[i],"',value: ",track$value[i],"});}",sep=''))
}
print("window.onload = function() {")
for(i in seq_len(nrow(track))){
  print(paste("if(document.getElementById('",track$id[i],"')) {document.getElementById('",track$id[i],"').onclick=segment",track$runningnumber[i],";}",sep=''))
}
print("}")
