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
  'urn:li:principal:datahub'
), (
  'urn:li:corpuser:datahub',
  'com.linkedin.identity.CorpUserEditableInfo',
  0,
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web/packages/data-portal/public/assets/images/default_avatar.png"}',
  now(),
  'urn:li:principal:datahub'
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

-- Some sample data, sourced from https://github.com/dalers/mywind.


SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

DROP SCHEMA IF EXISTS `northwind` ;
CREATE SCHEMA IF NOT EXISTS `northwind` DEFAULT CHARACTER SET latin1 ;
USE `northwind` ;

-- -----------------------------------------------------
-- Table `northwind`.`customers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`customers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;

-- Now, the actual sample data.

USE `northwind`;

#
# Dumping data for table 'customers'
#

INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`) VALUES (1, 'Company A', 'Bedecs', 'Anna', NULL);
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`) VALUES (2, 'Company B', 'Gratacos Solsona', 'Antonio', NULL);
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`) VALUES (3, 'Company C', 'Axen', 'Thomas', NULL);
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`) VALUES (4, 'Company D', 'Lee', 'Christina', NULL);
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`) VALUES (5, 'Company E', 'Donnell', 'Martin', NULL);
# 5 records

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;