
-- note that this may be resuming from a previous run, only do commands if things do not already exist.
CREATE DATABASE IF NOT EXISTS wherehows
  DEFAULT CHARACTER SET utf8
  DEFAULT COLLATE utf8_general_ci;

CREATE USER IF NOT EXISTS 'wherehows'@'localhost' IDENTIFIED BY 'wherehows';
CREATE USER IF NOT EXISTS 'wherehows'@'%' IDENTIFIED BY 'wherehows';

CREATE USER IF NOT EXISTS 'wherehows_ro'@'localhost' IDENTIFIED BY 'readmetadata';
CREATE USER IF NOT EXISTS 'wherehows_ro'@'%' IDENTIFIED BY 'readmetadata';

FLUSH PRIVILEGES;

GRANT ALL ON wherehows.* TO 'wherehows'@'localhost';
GRANT ALL ON wherehows.* TO 'wherehows'@'%';
GRANT SELECT ON wherehows.* TO 'wherehows_ro'@'localhost';
GRANT SELECT ON wherehows.* TO 'wherehows_ro'@'%';

FLUSH PRIVILEGES;
