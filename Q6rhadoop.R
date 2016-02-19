Sys.setenv(HADOOP_CMD='/usr/share/hadoop/bin/hadoop')
library("rmr2")
library("rhdfs")
hdfs.init()

Q6 <- function(input, output = NULL,pattern = ","){
  q6.map<-function(.,lines){
    res = NULL
    for (line in lines){
      res = c(res,unlist(strsplit(line,split=','))[4])
    }
    return(keyval(res, '1'))
  }

  q6.reduce<-function(k,v){
    keyval(k,toString(sum(as.integer(v))))
  }

  mapreduce(
    input = input,
    output = output,
    map = q6.map,
    reduce = q6.reduce,
    combine = FALSE,
    input.format=make.input.format("text"))
}

inputPath = '/user/hadoop/input/Customers'
outputPath = '/user/hadoop/out/rhadoop'

Q6(inputPath, outputPath)

results <- from.dfs(outputPath)

x <- results$key
y <- as.integer(results$val)

## Plot the values
barplot(y, main="Country Code Frequency", xlab="Code", ylab="Freq", names.arg=x)

## Sort the values and Plot them
z = sort(y,index.return = TRUE)$ix
barplot(y[z], main="Sorted Country Code Frequency", xlab="Code", ylab="Freq", names.arg=x[z], col="blue")
