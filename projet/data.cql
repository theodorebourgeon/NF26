CREATE KEYSPACE citybike WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};

CREATE KEYSPACE Projet_NF26 WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};



CREATE TABLE citybike.rent (
    id int PRIMARY KEY,
    bikeid int,
    birth_year int,
    end_station_id int,
    end_station_latitude double,
    end_station_longitude double,
    end_station_name text,
    gender int,
    start_station text,
    start_station_id int,
    start_station_latitude double,
    start_station_longitude double,
    starttime time,
    stoptime time,
    tripduration int,
    usertype text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';



create table Projet_NF26.asos_france_200
(
	station text,
	valid timestamp,
	lon float,
	lat float,
	tmpf float,
	dwpf float,
	relh float,
	drct int,
	sknt int,
	p01i float,
	alti float,
	mslp float,
	vsby float,
	gust int,
	skyc1 text,
	skyc2 text,
	skyc3 text,
	skyc4 text,
	skyl1 int,
	skyl2 int,
	skyl3 int,
	skyl4 int,
	wxcodes text,
	ice_accretion_1hr float,
	ice_accretion_3hr float,
	ice_accretion_6hr float,
	peak_wind_gust int,
	peak_wind_drct int,
	peak_wind_time timestamp,
	feel float,
	metar text,
    primary key (valid,lon,lat)
);


