# Set working directory to the directory containing the AirlineDelays data.
setwd("~/Stat480/RDataScience/AirlineDelays")

# Function to apply to a CSV file to convert a list of columns to integer index values.
# 
convertCSVColumns <- function(file, collist){
  fulldata<-read.csv(file)
  for (i in collist) {
    fulldata[,i]<-convertColumn(fulldata[,i])
  }
  write.csv(fulldata, file, row.names=FALSE)
}

# The following function is called by convertCSVColumns. It converts a single column to integer indices.
convertColumn <- function(values){
  allvals<-as.character(values)
  valslist<-sort(unique(allvals))
  xx<-factor(allvals, valslist, labels=1:length(valslist))
  rm(allvals)
  rm(valslist)
  gc()
  as.numeric(levels(xx))[xx]
}

# Now use the function on the 2007-2008 data. This will take several minutes.
convertCSVColumns("AirlineData0708.csv", c(9,11,17,18))


library(biganalytics)
# Now that the data is processed, we can create a big matrix.
x <- read.big.matrix("AirlineData0708.csv", header = TRUE, 
                     backingfile = "air0708.bin",
                     descriptorfile = "air0708.desc",
                     type = "integer", extraCols = "age")