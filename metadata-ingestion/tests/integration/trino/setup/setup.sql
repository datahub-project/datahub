CREATE SCHEMA librarydb;

CREATE TABLE librarydb.book (
	id INTEGER NOT NULL,
	name VARCHAR ( 50 ) NOT NULL,
	author VARCHAR ( 50 ),
  publisher VARCHAR (50),
  tags JSON,
  genre_ids INTEGER[],
  PRIMARY KEY (id)
);

CREATE TABLE librarydb.member (
	id INTEGER NOT NULL,
	name VARCHAR ( 50 ) NOT NULL,
  PRIMARY KEY (id)
);


CREATE TABLE librarydb.issue_history ( 
	book_id INTEGER NOT NULL, 
  member_id INTEGER NOT NULL,
  issue_date DATE,
  return_date DATE,
CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES librarydb.book(id),
CONSTRAINT fk_member FOREIGN KEY(member_id) REFERENCES librarydb.member(id)
);

INSERT INTO librarydb.book (id, name, author) VALUES (1, 'Book 1', 'ABC');
INSERT INTO librarydb.book (id, name, author) VALUES (2, 'Book 2', 'PQR');
INSERT INTO librarydb.book (id, name, author) VALUES (3, 'Book 3', 'XYZ');

INSERT INTO librarydb.member(id, name) VALUES (1, 'Member 1');
INSERT INTO librarydb.member(id, name) VALUES (2, 'Member 2');

INSERT INTO librarydb.issue_history VALUES (1, 1, TO_DATE('2021-09-27','YYYY-MM-DD'), TO_DATE('2021-09-27','YYYY-MM-DD'));
INSERT INTO librarydb.issue_history VALUES (2, 2, TO_DATE('2021-09-27','YYYY-MM-DD'), NULL);


CREATE VIEW librarydb.book_in_circulation as 
  SELECT b.id, b.name, b.author, b.publisher, i.member_id, i.issue_date FROM 
  librarydb.book b 
  JOIN 
  librarydb.issue_history i 
  on b.id=i.book_id 
  where i.return_date is null;
