
CREATE TABLE Data (
	record_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	record_type_id INT,
	unit_id INT,
	creation_date TEXT,
	starting_date TEXT,
	ending_date TEXT,
	record_value REAL,
	FOREIGN KEY (record_type_id) REFERENCES RecordType(record_type_id),
	FOREIGN KEY (unit_id) REFERENCES UnitType(unit_id)
);

CREATE TABLE RecordType (
	record_type_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	record_type TEXT UNIQUE
);

CREATE TABLE UnitType (
	unit_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	unit TEXT UNIQUE
);

CREATE TABLE Sleep (
	record_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	date TEXT,
	from_date TEXT,
	to_date TEXT,
	bedtime TEXT,
	waketime TEXT,
	in_bed INT,
	awake INT,
	fell_asleep_in INT,
	num_sessions INT,
	asleep INT,
	efficiency INT,
	quality INT,
	deep INT,
	sleep_BPM REAL,
	day_BPM REAL,
	waking_BPM REAL,
	hrv INT,
	sleep_hrv INT,
	resp_avg REAL,
	resp_min REAL,
	resp_max REAL
)
