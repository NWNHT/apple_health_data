
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
