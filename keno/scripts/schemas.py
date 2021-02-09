DRAWINGS_SCHEMA = """
CREATE TABLE "drawings" (
	"id"	INTEGER UNIQUE,
	"date"	INTEGER,
	"high_bits"	UNSIGNED SMALL INTEGER NOT NULL,
	"low_bits"	UNSIGNED INTEGER NOT NULL,
	"number_string"	TEXT,
	PRIMARY KEY("id")
);
"""


WAGERS_SCHEMA = """
CREATE TABLE "wagers" (
	"id"	INTEGER PRIMARY KEY AUTOINCREMENT,
    "date" INTEGER,
	"draw_number_id"	INTEGER,
    "begin_draw"  INTEGER,
    "end_draw"  INTEGER,
    "qp"    UNSIGNED INTEGER,
    "ticket_cost" INTEGER,
	"numbers_wagered_id"	INTEGER,
	"numbers_matched"	TINY INT,
	"high_match_mask"	UNSIGNED SMALL INTEGER,
	"low_match_mask"	UNSIGNED INTEGER,
	"prize"	UNSIGNED INT,
	FOREIGN KEY("draw_number_id") REFERENCES "drawings"("id"),
	FOREIGN KEY("date") REFERENCES "drawings"("date"),
	FOREIGN KEY("numbers_wagered") REFERENCES "numbers_wagered"("id")
);
"""

NUMBERS_WAGERED_SCHEMA = """
CREATE TABLE "numbers_wagered" (
	"id"	INTEGER PRIMARY KEY AUTOINCREMENT,
	"number_string"	TEXT,
	"high_bits"	UNSIGNED SMALL INTEGER,
	"low_bits"	UNSIGNED INTEGER,
	"numbers_played"	TINY INTEGER
);
"""
