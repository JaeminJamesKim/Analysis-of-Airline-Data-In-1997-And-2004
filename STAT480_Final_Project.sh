# Set working directory to the directory containing the AirlineDelays data.
setwd("~/Stat480/RDataScience/AirlineDelays")

library(RSQLite)

# sqlite3 AirlineDelay1980s.sqlite3
# 
# /*  
#   after starting sqlite enter the following code to create the table, 
# populate the table, and then exit out of sqlite 
# */
#   CREATE TABLE AirlineDelay9704 (
#     Year int,
#     Month int,
#     DayofMonth int,
#     DayofWeek int,
#     DepTime int,
#     CRSDepTime int,
#     ArrTime int,
#     CRSArrTime int,
#     UniqueCarrier varchar(5),
#     FlightNum int,
#     TailNum varchar(8),
#     ActualElapsedTime int,
#     CRSElapsedTime int,
#     AirTime int,
#     ArrDelay int,
#     DepDelay int,
#     Origin varchar(3),
#     Dest varchar(3),
#     Distance int,
#     TaxiIn int,
#     TaxiOut int,
#     Cancelled int,
#     CancellationCode varchar(1),
#     Diverted varchar(1),
#     CarrierDelay int,
#     WeatherDelay int,
#     NASDelay int,
#     SecurityDelay int,
#     LateAircraftDelay int );
# -- define separator
# .separator ,
# -- import data into table
# .import AirlineDelay9704.csv AirlineDelay9704
# -- exit sqlite
# .exit

# sqlite3 AirlineDelay1997.sqlite3
# 
# CREATE TABLE AirlineDelay1997 (
#   Year int,
#   Month int,
#   DayofMonth int,
#   DayofWeek int,
#   DepTime int,
#   CRSDepTime int,
#   ArrTime int,
#   CRSArrTime int,
#   UniqueCarrier varchar(5),
#   FlightNum int,
#   TailNum varchar(8),
#   ActualElapsedTime int,
#   CRSElapsedTime int,
#   AirTime int,
#   ArrDelay int,
#   DepDelay int,
#   Origin varchar(3),
#   Dest varchar(3),
#   Distance int,
#   TaxiIn int,
#   TaxiOut int,
#   Cancelled int,
#   CancellationCode varchar(1),
#   Diverted varchar(1),
#   CarrierDelay int,
#   WeatherDelay int,
#   NASDelay int,
#   SecurityDelay int,
#   LateAircraftDelay int );
# -- define separator
# .separator ,
# -- import data into table
# .import 1997.csv AirlineDelay1997
# -- exit sqlite
# .exit
# 
# sqlite3 AirlineDelay2004.sqlite3
# 
# CREATE TABLE AirlineDelay2004 (
#   Year int,
#   Month int,
#   DayofMonth int,
#   DayofWeek int,
#   DepTime int,
#   CRSDepTime int,
#   ArrTime int,
#   CRSArrTime int,
#   UniqueCarrier varchar(5),
#   FlightNum int,
#   TailNum varchar(8),
#   ActualElapsedTime int,
#   CRSElapsedTime int,
#   AirTime int,
#   ArrDelay int,
#   DepDelay int,
#   Origin varchar(3),
#   Dest varchar(3),
#   Distance int,
#   TaxiIn int,
#   TaxiOut int,
#   Cancelled int,
#   CancellationCode varchar(1),
#   Diverted varchar(1),
#   CarrierDelay int,
#   WeatherDelay int,
#   NASDelay int,
#   SecurityDelay int,
#   LateAircraftDelay int );
# -- define separator
# .separator ,
# -- import data into table
# .import 2004.csv AirlineDelay2004
# -- exit sqlite
# .exit




delay9704.con <- dbConnect(RSQLite::SQLite(), dbname = "AirlineDelay9704.sqlite3")


delay97.con <- dbConnect(RSQLite::SQLite(), dbname = "AirlineDelay1997.sqlite3")

delay97a<-dbGetQuery(delay97.con, 
                 "SELECT COUNT(*) 
           FROM AirlineDelay1997")[1,1]-1
delay97b<-dbGetQuery(delay97.con, 
                 "SELECT COUNT(*) 
                 FROM AirlineDelay1997
                 WHERE DepDelay>15")[1,1]

delay97b/delay97a
#[1] 0.1650909


delay04.con <- dbConnect(RSQLite::SQLite(), dbname = "AirlineDelay2004.sqlite3")

delay04a<-dbGetQuery(delay04.con, 
                     "SELECT COUNT(*) 
           FROM AirlineDelay2004")[1,1]-1
delay04b<-dbGetQuery(delay04.con, 
                     "SELECT COUNT(*) 
                 FROM AirlineDelay2004
                 WHERE DepDelay>15")[1,1]

delay04b/delay04a
#[1] 0.1742835


totalct97<-dbGetQuery(delay97.con, 
                 "SELECT COUNT(*) 
           FROM AirlineDelay1997")
totalct97
#1  5411844

totalct04<-dbGetQuery(delay04.con, 
                 "SELECT COUNT(*) 
                 FROM AirlineDelay2004")
totalct04
#1  7129271

#==========================================================================================
CREATE TABLE airlinedelay97 (Year INT,
                           Month INT,
                             DayofMonth INT,
                             DayofWeek INT,
                             DepTime INT,
                             CRSDepTime INT,
                             ArrTime INT,
                             CRSArrTime INT,
                             UniqueCarrier STRING,
                             FlightNum INT,
                             TailNum STRING,
                             ActualElapsedTime INT,
                             CRSElapsedTime INT,
                             AirTime INT,
                             ArrDelay INT,
                             DepDelay INT,
                             Origin STRING,
                             Dest STRING,
                             Distance INT,
                             TaxiIn INT,
                             TaxiOut INT,
                             Cancelled INT,
                             CancellationCode STRING,
                             Diverted STRING,
                             CarrierDelay INT,
                             WeatherDelay INT,
                             NASDelay INT,
                             SecurityDelay INT,
                             LateAircraftDelay INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '1997.csv'
OVERWRITE INTO TABLE airlinedelay97;

DESCRIBE airlinedelay97;

SELECT * FROM airlinedelay97 LIMIT 5;

CREATE TABLE carriers(Code STRING, Description STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
);
LOAD DATA LOCAL INPATH 'carriers.csv'
OVERWRITE INTO TABLE carriers;

DESCRIBE carriers;

SELECT * FROM carriers LIMIT 5;

CREATE TABLE ad_carrier97 AS SELECT * FROM airlinedelay97, carriers WHERE airlinedelay97.UniqueCarrier = carriers.Code;

SELECT * FROM ad_carrier97 LIMIT 5;


CREATE TABLE airlinedelay04 (Year INT,
                           Month INT,
                             DayofMonth INT,
                             DayofWeek INT,
                             DepTime INT,
                             CRSDepTime INT,
                             ArrTime INT,
                             CRSArrTime INT,
                             UniqueCarrier STRING,
                             FlightNum INT,
                             TailNum STRING,
                             ActualElapsedTime INT,
                             CRSElapsedTime INT,
                             AirTime INT,
                             ArrDelay INT,
                             DepDelay INT,
                             Origin STRING,
                             Dest STRING,
                             Distance INT,
                             TaxiIn INT,
                             TaxiOut INT,
                             Cancelled INT,
                             CancellationCode STRING,
                             Diverted STRING,
                             CarrierDelay INT,
                             WeatherDelay INT,
                             NASDelay INT,
                             SecurityDelay INT,
                             LateAircraftDelay INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '2004.csv'
OVERWRITE INTO TABLE airlinedelay04;

CREATE TABLE ad_carrier04 AS SELECT * FROM airlinedelay04, carriers WHERE airlinedelay04.UniqueCarrier = carriers.Code;

#=============================================================================
#Top 10 airlines with the most severe DEPARTURE delays in '97
SELECT Description, MAX(DepDelay) as d
FROM ad_carrier97 
GROUP BY Description 
ORDER BY d DESC, Description
LIMIT 10;
# Northwest Airlines Inc.	1618
# Delta Air Lines Inc.	1437
# American Airlines Inc.	1358
# Continental Air Lines Inc.	1187
# United Air Lines Inc.	984
# Trans World Airways LLC	731
# America West Airlines Inc. (Merged with US Airways 9/05. Stopped reporting 10/07.)	665
# Alaska Airlines Inc.	652
# Southwest Airlines Co.	580
# US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)	483


#Top 10 airlines with the most severe ARRIVAL delays in '97
SELECT Description, MAX(ArrDelay) as d
FROM ad_carrier97 
GROUP BY Description 
ORDER BY d DESC, Description
LIMIT 10;
# Northwest Airlines Inc.	1609
# American Airlines Inc.	1364
# Continental Air Lines Inc.	1178
# United Air Lines Inc.	972
# Trans World Airways LLC	774
# America West Airlines Inc. (Merged with US Airways 9/05. Stopped reporting 10/07.)	662
# Alaska Airlines Inc.	646
# Delta Air Lines Inc.	569
# Southwest Airlines Co.	568
# US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)	508

#Top 10 airlines with the most severe DEPARTURE delays in '04
SELECT Description, MAX(DepDelay) as d
FROM ad_carrier04 
GROUP BY Description 
ORDER BY d DESC, Description
LIMIT 10;
# Northwest Airlines Inc.	1882
# United Air Lines Inc.	1405
# American Airlines Inc.	1376
# Atlantic Southeast Airlines	1200
# American Eagle Airlines Inc.	1093
# America West Airlines Inc. (Merged with US Airways 9/05. Stopped reporting 10/07.)	1071
# Independence Air	1050
# Continental Air Lines Inc.	1008
# US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)	958
# JetBlue Airways	834

#Top 10 airlines with the most severe ARRIVAL delays in '04
SELECT Description, MAX(ArrDelay) as d
FROM ad_carrier04 
GROUP BY Description 
ORDER BY d DESC, Description
LIMIT 10;
# Northwest Airlines Inc.	1879
# United Air Lines Inc.	1393
# Comair Inc.	1380
# American Airlines Inc.	1379
# Atlantic Southeast Airlines	1187
# American Eagle Airlines Inc.	1086
# Continental Air Lines Inc.	1033
# ATA Airlines d/b/a ATA	993
# US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)	966
# Alaska Airlines Inc.	871


#=============================================================================

# Top 10 airlines with the most frequent departure delays in '97
select Description, count(*) as cnt
from ad_carrier97
where DepDelay>0
group by Description
order by cnt desc
limit 10;
# Delta Air Lines Inc.	481009
# United Air Lines Inc.	412867
# US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)	317819
# Southwest Airlines Co.	290235
# American Airlines Inc.	246490
# Northwest Airlines Inc.	212544
# Continental Air Lines Inc.	169227
# Trans World Airways LLC	82727
# America West Airlines Inc. (Merged with US Airways 9/05. Stopped reporting 10/07.)	64982
# Alaska Airlines Inc.	49442

# Top 10 airlines with the most frequent arrival delays in '97
select Description, count(*) as cnt
from ad_carrier97
where ArrDelay>0
group by Description
order by cnt desc
limit 10;
# Delta Air Lines Inc.	541877
# United Air Lines Inc.	380420
# Southwest Airlines Co.	367673
# US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)	346563
# American Airlines Inc.	284277
# Northwest Airlines Inc.	266367
# Continental Air Lines Inc.	200131
# Trans World Airways LLC	119785
# America West Airlines Inc. (Merged with US Airways 9/05. Stopped reporting 10/07.)	117416
# Alaska Airlines Inc.	88899



# Top 10 airlines with the most frequent departure delays in '04
select Description, count(*) as cnt
from ad_carrier04
where DepDelay>0
group by Description
order by cnt desc
limit 10;
# Southwest Airlines Co.	356750
# Delta Air Lines Inc.	304032
# American Airlines Inc.	232436
# United Air Lines Inc.	167732
# US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)	159393
# American Eagle Airlines Inc.	152217
# Northwest Airlines Inc.	145248
# Skywest Airlines Inc.	140844
# Comair Inc.	111595
# Expressjet Airlines Inc.	93500

# Top 10 airlines with the most frequent arrival delays in '04
select Description, count(*) as cnt
from ad_carrier04
where ArrDelay>0
group by Description
order by cnt desc
limit 10;
# Southwest Airlines Co.	411212
# Delta Air Lines Inc.	342552
# American Airlines Inc.	293269
# Northwest Airlines Inc.	233550
# United Air Lines Inc.	221798
# American Eagle Airlines Inc.	211377
# Skywest Airlines Inc.	179471
# Expressjet Airlines Inc.	165784
# US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)	161487
# Comair Inc.	143576

#=============================================================================
#Plane spec comparisons

CREATE TABLE planedata (tailnum STRING, type STRING, manufacturer STRING, issue_date DATE, model STRING, status STRING, aircraft_type STRING, engine_type STRING, year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH 'plane-data.csv'
OVERWRITE INTO TABLE planedata;

ALTER TABLE planedata CHANGE year planeyear INT;
ALTER TABLE planedata CHANGE tailnum planetailnum STRING;

CREATE TABLE airline_plane97 AS SELECT * FROM airlinedelay97, planedata WHERE airlinedelay97.TailNum = planedata.planetailnum;
CREATE TABLE airline_plane04 AS SELECT * FROM airlinedelay04, planedata WHERE airlinedelay04.TailNum = planedata.planetailnum;

CREATE TABLE airline_plane_carrier97 AS SELECT * FROM airline_plane97, carriers WHERE airline_plane97.UniqueCarrier = carriers.Code;
CREATE TABLE airline_plane_carrier04 AS SELECT * FROM airline_plane04, carriers WHERE airline_plane04.UniqueCarrier = carriers.Code;


#Delta's most frequent plane spec(Highest delay rate)
SELECT type, manufacturer, issue_date, model, status, aircraft_type, engine_type, planeyear, Description, count(*) as cnt
FROM airline_plane_carrier97
WHERE Description='Delta Air Lines Inc.'
GROUP BY Description, type, manufacturer, issue_date, model, status, aircraft_type, engine_type, planeyear
ORDER BY cnt DESC, Description
LIMIT 10;
# Corporation	MCDONNELL DOUGLAS AIRCRAFT CO	NULL	MD-88	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1991	Delta Air Lines Inc.	54778
# Corporation	MCDONNELL DOUGLAS AIRCRAFT CO	NULL	MD-88	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1990	Delta Air Lines Inc.	48496
# Corporation	MCDONNELL DOUGLAS AIRCRAFT CO	NULL	MD-88	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1988	Delta Air Lines Inc.	41707
# Corporation	MCDONNELL DOUGLAS AIRCRAFT CO	NULL	MD-88	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1989	Delta Air Lines Inc.	37852
# Corporation	MCDONNELL DOUGLAS CORPORATION	NULL	MD-88	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1992	Delta Air Lines Inc.	28828
# Corporation	MCDONNELL DOUGLAS AIRCRAFT CO	NULL	MD-88	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1987	Delta Air Lines Inc.	19463
# Corporation	BOEING	NULL	757-232	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1987	Delta Air Lines Inc.	18267
# Corporation	MCDONNELL DOUGLAS AIRCRAFT CO	NULL	MD-88	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1993	Delta Air Lines Inc.	17949
# Corporation	MCDONNELL DOUGLAS	NULL	MD-90-30	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1995	Delta Air Lines Inc.	17159

# SELECT manufacturer, count(DepDelay) as cnt
# FROM airline_plane_carrier97
# WHERE DepDelay>0 AND manufacturer IS NOT NULL
# GROUP BY manufacturer
# ORDER BY cnt DESC
# LIMIT 10;

# SELECT manufacturer, count(DepDelay) as cnt
# FROM airline_plane_carrier04
# WHERE DepDelay>0 AND manufacturer IS NOT NULL
# GROUP BY manufacturer
# ORDER BY cnt DESC
# LIMIT 10;


#Alaska AIrline's most frequent plane spec(10th highest delay rate)
SELECT type, manufacturer, issue_date, model, status, aircraft_type, engine_type, planeyear, Description, count(*) as cnt
FROM airline_plane_carrier97
WHERE Description='Alaska Airlines Inc.'
GROUP BY Description, type, manufacturer, issue_date, model, status, aircraft_type, engine_type, planeyear
ORDER BY cnt DESC, Description
LIMIT 10;
# Corporation	BOEING	NULL	737-4Q8	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1992	Alaska Airlines Inc.	16241
# Corporation	BOEING	NULL	737-4Q8	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1993	Alaska Airlines Inc.	12012
# Corporation	BOEING	NULL	737-4Q8	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1994	Alaska Airlines Inc.	11794
# Corporation	MCDONNELL DOUGLAS	NULL	DC-9-83(MD-83)	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1992	Alaska Airlines Inc.	5497
# Corporation	MCDONNELL DOUGLAS	NULL	DC-9-83(MD-83)	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1991	Alaska Airlines Inc.	4840
# Corporation	BOEING	NULL	737-490	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1992	Alaska Airlines Inc.	3911
# Corporation	MCDONNELL DOUGLAS	NULL	DC-9-83(MD-83)	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1990	Alaska Airlines Inc.	3608
# Corporation	MCDONNELL DOUGLAS	NULL	DC-9-83(MD-83)	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1994	Alaska Airlines Inc.	3598
# Corporation	BOEING	NULL	737-490	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1997	Alaska Airlines Inc.	2956
# Corporation	BOEING	NULL	737-4Q8	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1996	Alaska Airlines Inc.	2069

select Description, count(*) as cnt
from ad_carrier97
where ArrDelay=0
group by Description
order by cnt
limit 10;

SELECT type, manufacturer, issue_date, model, status, aircraft_type, engine_type, planeyear, Description, count(*) as cnt
FROM airline_plane_carrier04
WHERE Description='Southwest Airlines Co.'
GROUP BY Description, type, manufacturer, issue_date, model, status, aircraft_type, engine_type, planeyear
ORDER BY cnt DESC, Description
LIMIT 10;
# Corporation	BOEING	NULL	737-3H4	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1996	Southwest Airlines Co.	48941
# Corporation	BOEING	NULL	737-3H4	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1995	Southwest Airlines Co.	47496
# Corporation	BOEING	NULL	737-3H4	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1994	Southwest Airlines Co.	34643
# Corporation	BOEING	NULL	737-3H4	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1997	Southwest Airlines Co.	33259
# Corporation	BOEING	NULL	737-7H4	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1998	Southwest Airlines Co.	26368
# Corporation	BOEING	NULL	737-7H4	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1999	Southwest Airlines Co.	13572
# Corporation	BOEING	NULL	737-3H4	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1985	Southwest Airlines Co.	12594
# Corporation	BOEING	NULL	737-3G7	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1988	Southwest Airlines Co.	8845
# Corporation	BOEING	NULL	737-3Q8	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1985	Southwest Airlines Co.	8488
# Corporation	BOEING	NULL	737-3H4	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1984	Southwest Airlines Co.	6743

SELECT type, manufacturer, issue_date, model, status, aircraft_type, engine_type, planeyear, Description, count(*) as cnt
FROM airline_plane_carrier04
WHERE Description='Expressjet Airlines Inc.'
GROUP BY Description, type, manufacturer, issue_date, model, status, aircraft_type, engine_type, planeyear
ORDER BY cnt DESC, Description
LIMIT 10;

# Corporation	EMBRAER	NULL	EMB-145LR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	2001	Expressjet Airlines Inc.   48872
# Corporation	EMBRAER	NULL	EMB-145LR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	2002	Expressjet Airlines Inc.   47993
# Corporation	EMBRAER	NULL	EMB-145XR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	2003	Expressjet Airlines Inc.   43124
# Corporation	EMBRAER	NULL	EMB-145LR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	2000	Expressjet Airlines Inc.   36145
# Corporation	EMBRAER	NULL	EMB-145LR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1999	Expressjet Airlines Inc.   32225
# Corporation	EMBRAER	NULL	EMB-145EP	Valid	Fixed Wing Multi-Engine	Turbo-Jet	1997	Expressjet Airlines Inc.   25389
# Corporation	EMBRAER	NULL	EMB-145XR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	2002	Expressjet Airlines Inc.   23069
# Corporation	EMBRAER	NULL	EMB-145LR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	1998	Expressjet Airlines Inc.   19801
# Corporation	EMBRAER	NULL	EMB-135LR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	2001	Expressjet Airlines Inc.   16361
# Corporation	EMBRAER	NULL	EMB-145XR	Valid	Fixed Wing Multi-Engine	Turbo-Fan	2004	Expressjet Airlines Inc.   12665


#=======================================================================
#Location Data

CREATE TABLE airport_origin (iata_origin STRING, airport_origin STRING, city_origin STRING, state_origin STRING, country_origin STRING, lat_origin INT, long_origin INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
);
LOAD DATA LOCAL INPATH 'airports.csv'
OVERWRITE INTO TABLE airport_origin;


CREATE TABLE airport_dest (iata_dest STRING, airport_dest STRING, city_dest STRING, state_dest STRING, country_dest STRING, lat_dest INT, long_dest INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
);
LOAD DATA LOCAL INPATH 'airports.csv'
OVERWRITE INTO TABLE airport_dest;

CREATE TABLE airline_airport97 AS SELECT ad_carrier97.Year, ad_carrier97.Month, ad_carrier97.DayofMonth,ad_carrier97.DayofWeek,ad_carrier97.DepTime, ad_carrier97.ArrTime,ad_carrier97.UniqueCarrier,ad_carrier97.FlightNum,ad_carrier97.TailNum,ad_carrier97.ActualElapsedTime,ad_carrier97.AirTime, ad_carrier97.ArrDelay,ad_carrier97.DepDelay,ad_carrier97.Origin,ad_carrier97.Dest,ad_carrier97.Distance, airport_origin.*, airport_dest.*
FROM ad_carrier97, airport_origin, airport_dest WHERE ad_carrier97.Origin = airport_origin.iata_origin AND ad_carrier97.Dest = airport_dest.iata_dest;

CREATE TABLE airline_airport04 AS SELECT ad_carrier04.Year, ad_carrier04.Month, ad_carrier04.DayofMonth,ad_carrier04.DayofWeek,ad_carrier04.DepTime, ad_carrier04.ArrTime,ad_carrier04.UniqueCarrier,ad_carrier04.FlightNum,ad_carrier04.TailNum,ad_carrier04.ActualElapsedTime,ad_carrier04.AirTime, ad_carrier04.ArrDelay,ad_carrier04.DepDelay,ad_carrier04.Origin,ad_carrier04.Dest,ad_carrier04.Distance, airport_origin.*, airport_dest.*
FROM ad_carrier04, airport_origin, airport_dest WHERE ad_carrier04.Origin = airport_origin.iata_origin AND ad_carrier04.Dest = airport_dest.iata_dest;


DESCRIBE airline_airport97;
SELECT * FROM airline_airport97 LIMIT 5;

#THINK OF SUB DIVIDED IDEAS TO USE LOCATION DATA!

select Origin, count(depdelay)
from airline_airport97 
where depdelay>0
group by Origin;

select Dest, count(depdelay)
from airline_airport97 
where depdelay>0
group by Dest;

select Origin, count(depdelay)
from airline_airport04 
where depdelay>0
group by Origin;

select Dest, count(depdelay)
from airline_airport04 
where depdelay>0
group by Dest;


# hive -e 'select Origin, count(depdelay)
# from airline_airport97 
# where depdelay>0
# group by Origin;' | sed 's/[\t]/,/g'  > /home/airport97_depdelay_count.csv

Section6
treemap for regions

weather data with Florida airports by months (hurricanes)


select code, year, count(DepDelay) as dct
from combined_table
where DepDelay>0
group by code, year
order by dct desc;


create table combined_table as
select * from airline_plane_carrier97
union all
select * from airline_plane_carrier04;


select manufacturer, planeyear, count(DepDelay) as dct
from combined_table
where DepDelay>0
group by manufacturer, planeyear
order by dct desc;

# type                	string              	                    
# manufacturer        	string              	                    
# issue_date          	date                	                    
# model               	string              	                    
# status              	string              	                    
# aircraft_type       	string              	                    
# engine_type         	string              	                    
# planeyear           	int  


RELATE section2 with carriers!!!!



SELECT planeyear, manufacturer, depdelay
FROM combined_table
WHERE Description='Delta Air Lines Inc.' and depdelay>0 and planeyear is not null;


BOEING	120206
MCDONNELL DOUGLAS AIRCRAFT CO	115523
MCDONNELL DOUGLAS CORPORATION	15722
MCDONNELL DOUGLAS	13328


1991	45617
1988	37521
1990	36401
1989	30158
1992	29936
1987	23984
1993	15874
1995	10632
1985	9903
1984	8678
1986	7688
1996	4440
1998	1093
1994	892
1997	855


1992	8697
1994	5194
1993	4277
1997	2005
1990	1809
1991	1548
1996	1263
1995	565

BOEING	18134
MCDONNELL DOUGLAS	6255
MCDONNELL DOUGLAS AIRCRAFT CO	969



insert overwrite local directory 'Desktop/sec2.csv'
row format delimited
fields terminated by ','
SELECT planeyear, manufacturer, depdelay
FROM combined_table
WHERE Description='Delta Air Lines Inc.' and depdelay>0 and planeyear is not null;