INSERT INTO TABLE mytable 
    SELECT c1,c2 FROM
      (SELECT count(*) FROM test2) AS c1
    JOIN
      (SELECT count(*) FROM test3) AS c2;
