CREATE TABLE book (
	id INTEGER NOT NULL,
	name VARCHAR ( 50 ) NOT NULL,
	author VARCHAR ( 50 ),
  publisher VARCHAR (50),
  tags JSON,
  genre_ids INTEGER,
  PRIMARY KEY (id)
);

CREATE TABLE member (
	id INTEGER NOT NULL,
	name VARCHAR ( 50 ) NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE MixedCaseTable (
	id INTEGER NOT NULL,
	name VARCHAR ( 50 ) NOT NULL,
  PRIMARY KEY (id)
);


INSERT INTO book (id, name, author) VALUES (1, 'Book1', 'ABC');
INSERT INTO book (id, name, author) VALUES (2, 'Book2', 'PQR');
INSERT INTO book (id, name, author) VALUES (3, 'Book3', 'XYZ');

INSERT INTO member(id, name) VALUES (1, 'Member1');
INSERT INTO member(id, name) VALUES (2, 'Member2');

INSERT INTO MixedCaseTable(id, name) VALUES (2, 'Member2');
