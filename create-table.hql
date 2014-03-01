drop table if exists ais;
create external table if not exists ais (
objectid int,
lon double,
lat double,
soc int,
cog int,
heading int,
roation int,
zulu string,
status int,
voyageid int,
mmsi string,
receivertype string,
receiverid string,
draught int
) partitioned by (year int, month int, day int, hour int)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile;
