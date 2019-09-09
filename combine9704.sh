#!/bin/sh
#############################
## airlines_data.sh
#############################
## James Joseph Balamuta
## james.balamuta@gmail.com
#############################
## Initial Release 1.0 -- 03/06/15
#############################
## The objective of this file is to download the airlines data set through stat-computing.org
## The script will inflate the .bz2 archive.
## Furthermore, the script will create a master archive called airlines.csv containing all years requested.
#############################
## # Obtain Script
## wget https://raw.githubusercontent.com/coatless/stat490uiuc/master/airlines/airlines_data.sh
## chmod u+x airlines_data.sh
## 
## # Run the script

cp airlines_1997.csv airlines.csv
tail -n+2 airlines_2004.csv >>airlines.csv


