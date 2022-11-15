create database metagalaxy;
use metagalaxy;
-- create metadata aspect table
create table metadata_aspect (
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  version                       bigint(20) not null,
  metadata                      longtext not null,
  createdon                     datetime(6) not null,
  createdby                     varchar(255) not null,
  createdfor                    varchar(255),
  constraint pk_metadata_aspect primary key (urn,aspect,version)
);

-- create default records for datahub user
insert into metadata_aspect (urn, aspect, version, metadata, createdon, createdby) values(
  'urn:li:corpuser:datahub',
  'com.linkedin.identity.CorpUserInfo',
  0,
  '{"displayName":"Data Hub","active":true,"fullName":"Data Hub","email":"datahub@linkedin.com"}',
  now(),
  'urn:li:corpuser:__datahub_system'
), (
  'urn:li:corpuser:datahub',
  'com.linkedin.identity.CorpUserEditableInfo',
  0,
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png"}',
  now(),
  'urn:li:corpuser:__datahub_system'
);

-- create metadata index table
CREATE TABLE metadata_index (
 `id` BIGINT NOT NULL AUTO_INCREMENT,
 `urn` VARCHAR(200) NOT NULL COMMENT "This is a column comment about URNs",
 `aspect` VARCHAR(150) NOT NULL,
 `path` VARCHAR(150) NOT NULL,
 `longVal` BIGINT,
 `stringVal` VARCHAR(200),
 `doubleVal` DOUBLE,
 CONSTRAINT id_pk PRIMARY KEY (id),
 INDEX longIndex (`urn`,`aspect`,`path`,`longVal`),
 INDEX stringIndex (`urn`,`aspect`,`path`,`stringVal`),
 INDEX doubleIndex (`urn`,`aspect`,`path`,`doubleVal`)
) COMMENT="This is a table comment";

-- create view for testing
CREATE VIEW metadata_index_view AS SELECT id, urn, path, doubleVal FROM metadata_index;

-- -----------------------------------------------------
-- Some sample data, from https://github.com/datacharmer/test_db.
-- -----------------------------------------------------

CREATE SCHEMA IF NOT EXISTS `datacharmer` ;
USE `datacharmer` ;

CREATE TABLE `datacharmer`.`employees` (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      ENUM ('M','F')  NOT NULL,    
    hire_date   DATE            NOT NULL,
    PRIMARY KEY (emp_no)
);

CREATE TABLE `datacharmer`.`salaries` (
    emp_no      INT             NOT NULL,
    salary      INT             NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL,
    # FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no, from_date)
); 

-- now some actual data (10 employees and associated salary data)

INSERT INTO `employees` VALUES (10001,'1953-09-02','Georgi','Facello','M','1986-06-26'),
(10002,'1964-06-02','Bezalel','Simmel','F','1985-11-21'),
(10003,'1959-12-03','Parto','Bamford','M','1986-08-28'),
(10004,'1954-05-01','Chirstian','Koblick','M','1986-12-01'),
(10005,'1955-01-21','Kyoichi','Maliniak','M','1989-09-12'),
(10006,'1953-04-20','Anneke','Preusig','F','1989-06-02'),
(10007,'1957-05-23','Tzvetan','Zielinski','F','1989-02-10'),
(10008,'1958-02-19','Saniya','Kalloufi','M','1994-09-15'),
(10009,'1952-04-19','Sumant','Peac','F','1985-02-18'),
(10010,'1963-06-01','Duangkaew','Piveteau','F','1989-08-24');

INSERT INTO `salaries` VALUES (10001,60117,'1986-06-26','1987-06-26'),
(10001,62102,'1987-06-26','1988-06-25'),
(10001,66074,'1988-06-25','1989-06-25'),
(10001,66596,'1989-06-25','1990-06-25'),
(10001,66961,'1990-06-25','1991-06-25'),
(10001,71046,'1991-06-25','1992-06-24'),
(10001,74333,'1992-06-24','1993-06-24'),
(10001,75286,'1993-06-24','1994-06-24'),
(10001,75994,'1994-06-24','1995-06-24'),
(10001,76884,'1995-06-24','1996-06-23'),
(10001,80013,'1996-06-23','1997-06-23'),
(10001,81025,'1997-06-23','1998-06-23'),
(10001,81097,'1998-06-23','1999-06-23'),
(10001,84917,'1999-06-23','2000-06-22'),
(10001,85112,'2000-06-22','2001-06-22'),
(10001,85097,'2001-06-22','2002-06-22'),
(10001,88958,'2002-06-22','9999-01-01'),
(10002,65909,'1996-08-03','1997-08-03'),
(10002,65909,'1997-08-03','1998-08-03'),
(10002,67534,'1998-08-03','1999-08-03'),
(10002,69366,'1999-08-03','2000-08-02'),
(10002,71963,'2000-08-02','2001-08-02'),
(10002,72527,'2001-08-02','9999-01-01'),
(10003,40006,'1995-12-03','1996-12-02'),
(10003,43616,'1996-12-02','1997-12-02'),
(10003,43466,'1997-12-02','1998-12-02'),
(10003,43636,'1998-12-02','1999-12-02'),
(10003,43478,'1999-12-02','2000-12-01'),
(10003,43699,'2000-12-01','2001-12-01'),
(10003,43311,'2001-12-01','9999-01-01'),
(10004,40054,'1986-12-01','1987-12-01'),
(10004,42283,'1987-12-01','1988-11-30'),
(10004,42542,'1988-11-30','1989-11-30'),
(10004,46065,'1989-11-30','1990-11-30'),
(10004,48271,'1990-11-30','1991-11-30'),
(10004,50594,'1991-11-30','1992-11-29'),
(10004,52119,'1992-11-29','1993-11-29'),
(10004,54693,'1993-11-29','1994-11-29'),
(10004,58326,'1994-11-29','1995-11-29'),
(10004,60770,'1995-11-29','1996-11-28'),
(10004,62566,'1996-11-28','1997-11-28'),
(10004,64340,'1997-11-28','1998-11-28'),
(10004,67096,'1998-11-28','1999-11-28'),
(10004,69722,'1999-11-28','2000-11-27'),
(10004,70698,'2000-11-27','2001-11-27'),
(10004,74057,'2001-11-27','9999-01-01'),
(10005,78228,'1989-09-12','1990-09-12'),
(10005,82621,'1990-09-12','1991-09-12'),
(10005,83735,'1991-09-12','1992-09-11'),
(10005,85572,'1992-09-11','1993-09-11'),
(10005,85076,'1993-09-11','1994-09-11'),
(10005,86050,'1994-09-11','1995-09-11'),
(10005,88448,'1995-09-11','1996-09-10'),
(10005,88063,'1996-09-10','1997-09-10'),
(10005,89724,'1997-09-10','1998-09-10'),
(10005,90392,'1998-09-10','1999-09-10'),
(10005,90531,'1999-09-10','2000-09-09'),
(10005,91453,'2000-09-09','2001-09-09'),
(10005,94692,'2001-09-09','9999-01-01'),
(10006,40000,'1990-08-05','1991-08-05'),
(10006,42085,'1991-08-05','1992-08-04'),
(10006,42629,'1992-08-04','1993-08-04'),
(10006,45844,'1993-08-04','1994-08-04'),
(10006,47518,'1994-08-04','1995-08-04'),
(10006,47917,'1995-08-04','1996-08-03'),
(10006,52255,'1996-08-03','1997-08-03'),
(10006,53747,'1997-08-03','1998-08-03'),
(10006,56032,'1998-08-03','1999-08-03'),
(10006,58299,'1999-08-03','2000-08-02'),
(10006,60098,'2000-08-02','2001-08-02'),
(10006,59755,'2001-08-02','9999-01-01'),
(10007,56724,'1989-02-10','1990-02-10'),
(10007,60740,'1990-02-10','1991-02-10'),
(10007,62745,'1991-02-10','1992-02-10'),
(10007,63475,'1992-02-10','1993-02-09'),
(10007,63208,'1993-02-09','1994-02-09'),
(10007,64563,'1994-02-09','1995-02-09'),
(10007,68833,'1995-02-09','1996-02-09'),
(10007,70220,'1996-02-09','1997-02-08'),
(10007,73362,'1997-02-08','1998-02-08'),
(10007,75582,'1998-02-08','1999-02-08'),
(10007,79513,'1999-02-08','2000-02-08'),
(10007,80083,'2000-02-08','2001-02-07'),
(10007,84456,'2001-02-07','2002-02-07'),
(10007,88070,'2002-02-07','9999-01-01'),
(10008,46671,'1998-03-11','1999-03-11'),
(10008,48584,'1999-03-11','2000-03-10'),
(10008,52668,'2000-03-10','2000-07-31'),
(10009,60929,'1985-02-18','1986-02-18'),
(10009,64604,'1986-02-18','1987-02-18'),
(10009,64780,'1987-02-18','1988-02-18'),
(10009,66302,'1988-02-18','1989-02-17'),
(10009,69042,'1989-02-17','1990-02-17'),
(10009,70889,'1990-02-17','1991-02-17'),
(10009,71434,'1991-02-17','1992-02-17'),
(10009,74612,'1992-02-17','1993-02-16'),
(10009,76518,'1993-02-16','1994-02-16'),
(10009,78335,'1994-02-16','1995-02-16'),
(10009,80944,'1995-02-16','1996-02-16'),
(10009,82507,'1996-02-16','1997-02-15'),
(10009,85875,'1997-02-15','1998-02-15'),
(10009,89324,'1998-02-15','1999-02-15'),
(10009,90668,'1999-02-15','2000-02-15'),
(10009,93507,'2000-02-15','2001-02-14'),
(10009,94443,'2001-02-14','2002-02-14'),
(10009,94409,'2002-02-14','9999-01-01'),
(10010,72488,'1996-11-24','1997-11-24'),
(10010,74347,'1997-11-24','1998-11-24'),
(10010,75405,'1998-11-24','1999-11-24'),
(10010,78194,'1999-11-24','2000-11-23'),
(10010,79580,'2000-11-23','2001-11-23'),
(10010,80324,'2001-11-23','9999-01-01');

-- -----------------------------------------------------
-- Some sample data, modified from https://github.com/dalers/mywind.
-- -----------------------------------------------------

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

DROP SCHEMA IF EXISTS `northwind` ;
CREATE SCHEMA IF NOT EXISTS `northwind` DEFAULT CHARACTER SET latin1 ;
USE `northwind` ;

-- Table `northwind`.`customers`
CREATE TABLE IF NOT EXISTS `northwind`.`customers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  `priority` FLOAT NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

-- Table `northwind`.`orders`
CREATE TABLE IF NOT EXISTS `northwind`.`orders` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `description` VARCHAR(50) NULL DEFAULT NULL,
  `customer_id` INT(11) NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_order_customer`
    FOREIGN KEY (`customer_id`)
    REFERENCES `northwind`.`customers`(`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

-- Now, the actual sample data.

USE `northwind`;

#
# Dumping data for table 'customers'
#

INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `priority`) VALUES (1, 'Company A', 'Bedecs', 'Anna', NULL, 4);
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `priority`) VALUES (2, 'Company B', 'Gratacos Solsona', 'Antonio', NULL, 4.9);
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `priority`) VALUES (3, 'Company C', 'Axen', 'Thomas', NULL, 4);
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `priority`) VALUES (4, 'Company D', 'Lee', 'Christina', NULL, 3.8);
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `priority`) VALUES (5, 'Company E', 'Donnell', 'Martin', NULL, NULL);
# 5 records

-- -----------------------------------------------------
-- Schema for testing different scenarios
-- -----------------------------------------------------

DROP SCHEMA IF EXISTS `test_cases` ;
CREATE SCHEMA IF NOT EXISTS `test_cases` DEFAULT CHARACTER SET latin1 ;
USE `test_cases` ;

-- no data in `test_cases`.`test_empty`

CREATE TABLE IF NOT EXISTS `test_cases`.`test_empty` (
  `dummy` VARCHAR(50) NULL DEFAULT NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

CREATE TABLE IF NOT EXISTS `test_cases`.`myset` (col SET('a', 'b', 'c', 'd'));

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
