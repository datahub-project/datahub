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
  `job_title` VARCHAR(50) NULL DEFAULT NULL,
  `business_phone` VARCHAR(25) NULL DEFAULT NULL,
  `home_phone` VARCHAR(25) NULL DEFAULT NULL,
  `mobile_phone` VARCHAR(25) NULL DEFAULT NULL,
  `fax_number` VARCHAR(25) NULL DEFAULT NULL,
  `address` LONGTEXT NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `state_province` VARCHAR(50) NULL DEFAULT NULL,
  `zip_postal_code` VARCHAR(15) NULL DEFAULT NULL,
  `country_region` VARCHAR(50) NULL DEFAULT NULL,
  `web_page` LONGTEXT NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `city` (`city` ASC),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC),
  INDEX `zip_postal_code` (`zip_postal_code` ASC),
  INDEX `state_province` (`state_province` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`employees`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`employees` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  `job_title` VARCHAR(50) NULL DEFAULT NULL,
  `business_phone` VARCHAR(25) NULL DEFAULT NULL,
  `home_phone` VARCHAR(25) NULL DEFAULT NULL,
  `mobile_phone` VARCHAR(25) NULL DEFAULT NULL,
  `fax_number` VARCHAR(25) NULL DEFAULT NULL,
  `address` LONGTEXT NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `state_province` VARCHAR(50) NULL DEFAULT NULL,
  `zip_postal_code` VARCHAR(15) NULL DEFAULT NULL,
  `country_region` VARCHAR(50) NULL DEFAULT NULL,
  `web_page` LONGTEXT NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `city` (`city` ASC),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC),
  INDEX `zip_postal_code` (`zip_postal_code` ASC),
  INDEX `state_province` (`state_province` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`privileges`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`privileges` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `privilege_name` VARCHAR(50) NULL DEFAULT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`employee_privileges`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`employee_privileges` (
  `employee_id` INT(11) NOT NULL,
  `privilege_id` INT(11) NOT NULL,
  PRIMARY KEY (`employee_id`, `privilege_id`),
  INDEX `employee_id` (`employee_id` ASC),
  INDEX `privilege_id` (`privilege_id` ASC),
  INDEX `privilege_id_2` (`privilege_id` ASC),
  CONSTRAINT `fk_employee_privileges_employees1`
    FOREIGN KEY (`employee_id`)
    REFERENCES `northwind`.`employees` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_employee_privileges_privileges1`
    FOREIGN KEY (`privilege_id`)
    REFERENCES `northwind`.`privileges` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`inventory_transaction_types`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`inventory_transaction_types` (
  `id` TINYINT(4) NOT NULL,
  `type_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`shippers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`shippers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  `job_title` VARCHAR(50) NULL DEFAULT NULL,
  `business_phone` VARCHAR(25) NULL DEFAULT NULL,
  `home_phone` VARCHAR(25) NULL DEFAULT NULL,
  `mobile_phone` VARCHAR(25) NULL DEFAULT NULL,
  `fax_number` VARCHAR(25) NULL DEFAULT NULL,
  `address` LONGTEXT NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `state_province` VARCHAR(50) NULL DEFAULT NULL,
  `zip_postal_code` VARCHAR(15) NULL DEFAULT NULL,
  `country_region` VARCHAR(50) NULL DEFAULT NULL,
  `web_page` LONGTEXT NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `city` (`city` ASC),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC),
  INDEX `zip_postal_code` (`zip_postal_code` ASC),
  INDEX `state_province` (`state_province` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`orders_tax_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`orders_tax_status` (
  `id` TINYINT(4) NOT NULL,
  `tax_status_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`orders_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`orders_status` (
  `id` TINYINT(4) NOT NULL,
  `status_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`orders`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`orders` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `employee_id` INT(11) NULL DEFAULT NULL,
  `customer_id` INT(11) NULL DEFAULT NULL,
  `order_date` DATETIME NULL DEFAULT NULL,
  `shipped_date` DATETIME NULL DEFAULT NULL,
  `shipper_id` INT(11) NULL DEFAULT NULL,
  `ship_name` VARCHAR(50) NULL DEFAULT NULL,
  `ship_address` LONGTEXT NULL DEFAULT NULL,
  `ship_city` VARCHAR(50) NULL DEFAULT NULL,
  `ship_state_province` VARCHAR(50) NULL DEFAULT NULL,
  `ship_zip_postal_code` VARCHAR(50) NULL DEFAULT NULL,
  `ship_country_region` VARCHAR(50) NULL DEFAULT NULL,
  `shipping_fee` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `taxes` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `payment_type` VARCHAR(50) NULL DEFAULT NULL,
  `paid_date` DATETIME NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `tax_rate` DOUBLE NULL DEFAULT '0',
  `tax_status_id` TINYINT(4) NULL DEFAULT NULL,
  `status_id` TINYINT(4) NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  INDEX `customer_id` (`customer_id` ASC),
  INDEX `customer_id_2` (`customer_id` ASC),
  INDEX `employee_id` (`employee_id` ASC),
  INDEX `employee_id_2` (`employee_id` ASC),
  INDEX `id` (`id` ASC),
  INDEX `id_2` (`id` ASC),
  INDEX `shipper_id` (`shipper_id` ASC),
  INDEX `shipper_id_2` (`shipper_id` ASC),
  INDEX `id_3` (`id` ASC),
  INDEX `tax_status` (`tax_status_id` ASC),
  INDEX `ship_zip_postal_code` (`ship_zip_postal_code` ASC),
  CONSTRAINT `fk_orders_customers`
    FOREIGN KEY (`customer_id`)
    REFERENCES `northwind`.`customers` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_orders_employees1`
    FOREIGN KEY (`employee_id`)
    REFERENCES `northwind`.`employees` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_orders_shippers1`
    FOREIGN KEY (`shipper_id`)
    REFERENCES `northwind`.`shippers` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_orders_orders_tax_status1`
    FOREIGN KEY (`tax_status_id`)
    REFERENCES `northwind`.`orders_tax_status` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_orders_orders_status1`
    FOREIGN KEY (`status_id`)
    REFERENCES `northwind`.`orders_status` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`products`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`products` (
  `supplier_ids` LONGTEXT NULL DEFAULT NULL,
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `product_code` VARCHAR(25) NULL DEFAULT NULL,
  `product_name` VARCHAR(50) NULL DEFAULT NULL,
  `description` LONGTEXT NULL DEFAULT NULL,
  `standard_cost` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `list_price` DECIMAL(19,4) NOT NULL DEFAULT '0.0000',
  `reorder_level` INT(11) NULL DEFAULT NULL,
  `target_level` INT(11) NULL DEFAULT NULL,
  `quantity_per_unit` VARCHAR(50) NULL DEFAULT NULL,
  `discontinued` TINYINT(1) NOT NULL DEFAULT '0',
  `minimum_reorder_quantity` INT(11) NULL DEFAULT NULL,
  `category` VARCHAR(50) NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `product_code` (`product_code` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`purchase_order_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`purchase_order_status` (
  `id` INT(11) NOT NULL,
  `status` VARCHAR(50) NULL DEFAULT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`suppliers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`suppliers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `company` VARCHAR(50) NULL DEFAULT NULL,
  `last_name` VARCHAR(50) NULL DEFAULT NULL,
  `first_name` VARCHAR(50) NULL DEFAULT NULL,
  `email_address` VARCHAR(50) NULL DEFAULT NULL,
  `job_title` VARCHAR(50) NULL DEFAULT NULL,
  `business_phone` VARCHAR(25) NULL DEFAULT NULL,
  `home_phone` VARCHAR(25) NULL DEFAULT NULL,
  `mobile_phone` VARCHAR(25) NULL DEFAULT NULL,
  `fax_number` VARCHAR(25) NULL DEFAULT NULL,
  `address` LONGTEXT NULL DEFAULT NULL,
  `city` VARCHAR(50) NULL DEFAULT NULL,
  `state_province` VARCHAR(50) NULL DEFAULT NULL,
  `zip_postal_code` VARCHAR(15) NULL DEFAULT NULL,
  `country_region` VARCHAR(50) NULL DEFAULT NULL,
  `web_page` LONGTEXT NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `attachments` LONGBLOB NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `city` (`city` ASC),
  INDEX `company` (`company` ASC),
  INDEX `first_name` (`first_name` ASC),
  INDEX `last_name` (`last_name` ASC),
  INDEX `zip_postal_code` (`zip_postal_code` ASC),
  INDEX `state_province` (`state_province` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`purchase_orders`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`purchase_orders` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `supplier_id` INT(11) NULL DEFAULT NULL,
  `created_by` INT(11) NULL DEFAULT NULL,
  `submitted_date` DATETIME NULL DEFAULT NULL,
  `creation_date` DATETIME NULL DEFAULT NULL,
  `status_id` INT(11) NULL DEFAULT '0',
  `expected_date` DATETIME NULL DEFAULT NULL,
  `shipping_fee` DECIMAL(19,4) NOT NULL DEFAULT '0.0000',
  `taxes` DECIMAL(19,4) NOT NULL DEFAULT '0.0000',
  `payment_date` DATETIME NULL DEFAULT NULL,
  `payment_amount` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `payment_method` VARCHAR(50) NULL DEFAULT NULL,
  `notes` LONGTEXT NULL DEFAULT NULL,
  `approved_by` INT(11) NULL DEFAULT NULL,
  `approved_date` DATETIME NULL DEFAULT NULL,
  `submitted_by` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id` (`id` ASC),
  INDEX `created_by` (`created_by` ASC),
  INDEX `status_id` (`status_id` ASC),
  INDEX `id_2` (`id` ASC),
  INDEX `supplier_id` (`supplier_id` ASC),
  INDEX `supplier_id_2` (`supplier_id` ASC),
  CONSTRAINT `fk_purchase_orders_employees1`
    FOREIGN KEY (`created_by`)
    REFERENCES `northwind`.`employees` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_purchase_orders_purchase_order_status1`
    FOREIGN KEY (`status_id`)
    REFERENCES `northwind`.`purchase_order_status` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_purchase_orders_suppliers1`
    FOREIGN KEY (`supplier_id`)
    REFERENCES `northwind`.`suppliers` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`inventory_transactions`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`inventory_transactions` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `transaction_type` TINYINT(4) NOT NULL,
  `transaction_created_date` DATETIME NULL DEFAULT NULL,
  `transaction_modified_date` DATETIME NULL DEFAULT NULL,
  `product_id` INT(11) NOT NULL,
  `quantity` INT(11) NOT NULL,
  `purchase_order_id` INT(11) NULL DEFAULT NULL,
  `customer_order_id` INT(11) NULL DEFAULT NULL,
  `comments` VARCHAR(255) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `customer_order_id` (`customer_order_id` ASC),
  INDEX `customer_order_id_2` (`customer_order_id` ASC),
  INDEX `product_id` (`product_id` ASC),
  INDEX `product_id_2` (`product_id` ASC),
  INDEX `purchase_order_id` (`purchase_order_id` ASC),
  INDEX `purchase_order_id_2` (`purchase_order_id` ASC),
  INDEX `transaction_type` (`transaction_type` ASC),
  CONSTRAINT `fk_inventory_transactions_orders1`
    FOREIGN KEY (`customer_order_id`)
    REFERENCES `northwind`.`orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_inventory_transactions_products1`
    FOREIGN KEY (`product_id`)
    REFERENCES `northwind`.`products` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_inventory_transactions_purchase_orders1`
    FOREIGN KEY (`purchase_order_id`)
    REFERENCES `northwind`.`purchase_orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_inventory_transactions_inventory_transaction_types1`
    FOREIGN KEY (`transaction_type`)
    REFERENCES `northwind`.`inventory_transaction_types` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`invoices`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`invoices` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `order_id` INT(11) NULL DEFAULT NULL,
  `invoice_date` DATETIME NULL DEFAULT NULL,
  `due_date` DATETIME NULL DEFAULT NULL,
  `tax` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `shipping` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `amount_due` DECIMAL(19,4) NULL DEFAULT '0.0000',
  PRIMARY KEY (`id`),
  INDEX `id` (`id` ASC),
  INDEX `id_2` (`id` ASC),
  INDEX `fk_invoices_orders1_idx` (`order_id` ASC),
  CONSTRAINT `fk_invoices_orders1`
    FOREIGN KEY (`order_id`)
    REFERENCES `northwind`.`orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`order_details_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`order_details_status` (
  `id` INT(11) NOT NULL,
  `status_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`order_details`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`order_details` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `order_id` INT(11) NOT NULL,
  `product_id` INT(11) NULL DEFAULT NULL,
  `quantity` DECIMAL(18,4) NOT NULL DEFAULT '0.0000',
  `unit_price` DECIMAL(19,4) NULL DEFAULT '0.0000',
  `discount` DOUBLE NOT NULL DEFAULT '0',
  `status_id` INT(11) NULL DEFAULT NULL,
  `date_allocated` DATETIME NULL DEFAULT NULL,
  `purchase_order_id` INT(11) NULL DEFAULT NULL,
  `inventory_id` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `id` (`id` ASC),
  INDEX `inventory_id` (`inventory_id` ASC),
  INDEX `id_2` (`id` ASC),
  INDEX `id_3` (`id` ASC),
  INDEX `id_4` (`id` ASC),
  INDEX `product_id` (`product_id` ASC),
  INDEX `product_id_2` (`product_id` ASC),
  INDEX `purchase_order_id` (`purchase_order_id` ASC),
  INDEX `id_5` (`id` ASC),
  INDEX `fk_order_details_orders1_idx` (`order_id` ASC),
  INDEX `fk_order_details_order_details_status1_idx` (`status_id` ASC),
  CONSTRAINT `fk_order_details_orders1`
    FOREIGN KEY (`order_id`)
    REFERENCES `northwind`.`orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_order_details_products1`
    FOREIGN KEY (`product_id`)
    REFERENCES `northwind`.`products` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_order_details_order_details_status1`
    FOREIGN KEY (`status_id`)
    REFERENCES `northwind`.`order_details_status` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`purchase_order_details`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`purchase_order_details` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `purchase_order_id` INT(11) NOT NULL,
  `product_id` INT(11) NULL DEFAULT NULL,
  `quantity` DECIMAL(18,4) NOT NULL,
  `unit_cost` DECIMAL(19,4) NOT NULL,
  `date_received` DATETIME NULL DEFAULT NULL,
  `posted_to_inventory` TINYINT(1) NOT NULL DEFAULT '0',
  `inventory_id` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `id` (`id` ASC),
  INDEX `inventory_id` (`inventory_id` ASC),
  INDEX `inventory_id_2` (`inventory_id` ASC),
  INDEX `purchase_order_id` (`purchase_order_id` ASC),
  INDEX `product_id` (`product_id` ASC),
  INDEX `product_id_2` (`product_id` ASC),
  INDEX `purchase_order_id_2` (`purchase_order_id` ASC),
  CONSTRAINT `fk_purchase_order_details_inventory_transactions1`
    FOREIGN KEY (`inventory_id`)
    REFERENCES `northwind`.`inventory_transactions` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_purchase_order_details_products1`
    FOREIGN KEY (`product_id`)
    REFERENCES `northwind`.`products` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_purchase_order_details_purchase_orders1`
    FOREIGN KEY (`purchase_order_id`)
    REFERENCES `northwind`.`purchase_orders` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`sales_reports`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`sales_reports` (
  `group_by` VARCHAR(50) NOT NULL,
  `display` VARCHAR(50) NULL DEFAULT NULL,
  `title` VARCHAR(50) NULL DEFAULT NULL,
  `filter_row_source` LONGTEXT NULL DEFAULT NULL,
  `default` TINYINT(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`group_by`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `northwind`.`strings`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `northwind`.`strings` (
  `string_id` INT(11) NOT NULL AUTO_INCREMENT,
  `string_data` VARCHAR(255) NULL DEFAULT NULL,
  PRIMARY KEY (`string_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

-- Now, the actual sample data.

USE `northwind`;

#
# Dumping data for table 'customers'
#

INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (1, 'Company A', 'Bedecs', 'Anna', NULL, 'Owner', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 1st Street', 'Seattle', 'WA', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (2, 'Company B', 'Gratacos Solsona', 'Antonio', NULL, 'Owner', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 2nd Street', 'Boston', 'MA', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (3, 'Company C', 'Axen', 'Thomas', NULL, 'Purchasing Representative', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 3rd Street', 'Los Angelas', 'CA', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (4, 'Company D', 'Lee', 'Christina', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 4th Street', 'New York', 'NY', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (5, 'Company E', 'O’Donnell', 'Martin', NULL, 'Owner', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 5th Street', 'Minneapolis', 'MN', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (6, 'Company F', 'Pérez-Olaeta', 'Francisco', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 6th Street', 'Milwaukee', 'WI', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (7, 'Company G', 'Xie', 'Ming-Yang', NULL, 'Owner', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 7th Street', 'Boise', 'ID', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (8, 'Company H', 'Andersen', 'Elizabeth', NULL, 'Purchasing Representative', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 8th Street', 'Portland', 'OR', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (9, 'Company I', 'Mortensen', 'Sven', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 9th Street', 'Salt Lake City', 'UT', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (10, 'Company J', 'Wacker', 'Roland', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 10th Street', 'Chicago', 'IL', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (11, 'Company K', 'Krschne', 'Peter', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 11th Street', 'Miami', 'FL', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (12, 'Company L', 'Edwards', 'John', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '123 12th Street', 'Las Vegas', 'NV', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (13, 'Company M', 'Ludick', 'Andre', NULL, 'Purchasing Representative', '(123)555-0100', NULL, NULL, '(123)555-0101', '456 13th Street', 'Memphis', 'TN', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (14, 'Company N', 'Grilo', 'Carlos', NULL, 'Purchasing Representative', '(123)555-0100', NULL, NULL, '(123)555-0101', '456 14th Street', 'Denver', 'CO', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (15, 'Company O', 'Kupkova', 'Helena', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '456 15th Street', 'Honolulu', 'HI', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (16, 'Company P', 'Goldschmidt', 'Daniel', NULL, 'Purchasing Representative', '(123)555-0100', NULL, NULL, '(123)555-0101', '456 16th Street', 'San Francisco', 'CA', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (17, 'Company Q', 'Bagel', 'Jean Philippe', NULL, 'Owner', '(123)555-0100', NULL, NULL, '(123)555-0101', '456 17th Street', 'Seattle', 'WA', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (18, 'Company R', 'Autier Miconi', 'Catherine', NULL, 'Purchasing Representative', '(123)555-0100', NULL, NULL, '(123)555-0101', '456 18th Street', 'Boston', 'MA', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (19, 'Company S', 'Eggerer', 'Alexander', NULL, 'Accounting Assistant', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 19th Street', 'Los Angelas', 'CA', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (20, 'Company T', 'Li', 'George', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 20th Street', 'New York', 'NY', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (21, 'Company U', 'Tham', 'Bernard', NULL, 'Accounting Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 21th Street', 'Minneapolis', 'MN', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (22, 'Company V', 'Ramos', 'Luciana', NULL, 'Purchasing Assistant', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 22th Street', 'Milwaukee', 'WI', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (23, 'Company W', 'Entin', 'Michael', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 23th Street', 'Portland', 'OR', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (24, 'Company X', 'Hasselberg', 'Jonas', NULL, 'Owner', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 24th Street', 'Salt Lake City', 'UT', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (25, 'Company Y', 'Rodman', 'John', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 25th Street', 'Chicago', 'IL', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (26, 'Company Z', 'Liu', 'Run', NULL, 'Accounting Assistant', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 26th Street', 'Miami', 'FL', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (27, 'Company AA', 'Toh', 'Karen', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 27th Street', 'Las Vegas', 'NV', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (28, 'Company BB', 'Raghav', 'Amritansh', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 28th Street', 'Memphis', 'TN', '99999', 'USA', NULL, NULL, '');
INSERT INTO `customers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (29, 'Company CC', 'Lee', 'Soo Jung', NULL, 'Purchasing Manager', '(123)555-0100', NULL, NULL, '(123)555-0101', '789 29th Street', 'Denver', 'CO', '99999', 'USA', NULL, NULL, '');
# 29 records

#
# Dumping data for table 'employee_privileges'
#

INSERT INTO `employee_privileges` (`employee_id`, `privilege_id`) VALUES (2, 2);
# 1 records

#
# Dumping data for table 'employees'
#

INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (1, 'Northwind Traders', 'Freehafer', 'Nancy', 'nancy@northwindtraders.com', 'Sales Representative', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 1st Avenue', 'Seattle', 'WA', '99999', 'USA', '#http://northwindtraders.com#', NULL, '');
INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (2, 'Northwind Traders', 'Cencini', 'Andrew', 'andrew@northwindtraders.com', 'Vice President, Sales', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 2nd Avenue', 'Bellevue', 'WA', '99999', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', 'Joined the company as a sales representative, was promoted to sales manager and was then named vice president of sales.', '');
INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (3, 'Northwind Traders', 'Kotas', 'Jan', 'jan@northwindtraders.com', 'Sales Representative', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 3rd Avenue', 'Redmond', 'WA', '99999', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', 'Was hired as a sales associate and was promoted to sales representative.', '');
INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (4, 'Northwind Traders', 'Sergienko', 'Mariya', 'mariya@northwindtraders.com', 'Sales Representative', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 4th Avenue', 'Kirkland', 'WA', '99999', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', NULL, '');
INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (5, 'Northwind Traders', 'Thorpe', 'Steven', 'steven@northwindtraders.com', 'Sales Manager', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 5th Avenue', 'Seattle', 'WA', '99999', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', 'Joined the company as a sales representative and was promoted to sales manager.  Fluent in French.', '');
INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (6, 'Northwind Traders', 'Neipper', 'Michael', 'michael@northwindtraders.com', 'Sales Representative', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 6th Avenue', 'Redmond', 'WA', '99999', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', 'Fluent in Japanese and can read and write French, Portuguese, and Spanish.', '');
INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (7, 'Northwind Traders', 'Zare', 'Robert', 'robert@northwindtraders.com', 'Sales Representative', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 7th Avenue', 'Seattle', 'WA', '99999', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', NULL, '');
INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (8, 'Northwind Traders', 'Giussani', 'Laura', 'laura@northwindtraders.com', 'Sales Coordinator', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 8th Avenue', 'Redmond', 'WA', '99999', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', 'Reads and writes French.', '');
INSERT INTO `employees` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (9, 'Northwind Traders', 'Hellung-Larsen', 'Anne', 'anne@northwindtraders.com', 'Sales Representative', '(123)555-0100', '(123)555-0102', NULL, '(123)555-0103', '123 9th Avenue', 'Seattle', 'WA', '99999', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', 'Fluent in French and German.', '');
# 9 records

#
# Dumping data for table 'inventory_transaction_types'
#

INSERT INTO `inventory_transaction_types` (`id`, `type_name`) VALUES (1, 'Purchased');
INSERT INTO `inventory_transaction_types` (`id`, `type_name`) VALUES (2, 'Sold');
INSERT INTO `inventory_transaction_types` (`id`, `type_name`) VALUES (3, 'On Hold');
INSERT INTO `inventory_transaction_types` (`id`, `type_name`) VALUES (4, 'Waste');
# 4 records

#
# Dumping data for table 'inventory_transactions'
#

INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (35, 1, '2006-03-22 16:02:28', '2006-03-22 16:02:28', 80, 75, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (36, 1, '2006-03-22 16:02:48', '2006-03-22 16:02:48', 72, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (37, 1, '2006-03-22 16:03:04', '2006-03-22 16:03:04', 52, 100, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (38, 1, '2006-03-22 16:03:09', '2006-03-22 16:03:09', 56, 120, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (39, 1, '2006-03-22 16:03:14', '2006-03-22 16:03:14', 57, 80, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (40, 1, '2006-03-22 16:03:40', '2006-03-22 16:03:40', 6, 100, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (41, 1, '2006-03-22 16:03:47', '2006-03-22 16:03:47', 7, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (42, 1, '2006-03-22 16:03:54', '2006-03-22 16:03:54', 8, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (43, 1, '2006-03-22 16:04:02', '2006-03-22 16:04:02', 14, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (44, 1, '2006-03-22 16:04:07', '2006-03-22 16:04:07', 17, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (45, 1, '2006-03-22 16:04:12', '2006-03-22 16:04:12', 19, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (46, 1, '2006-03-22 16:04:17', '2006-03-22 16:04:17', 20, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (47, 1, '2006-03-22 16:04:20', '2006-03-22 16:04:20', 21, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (48, 1, '2006-03-22 16:04:24', '2006-03-22 16:04:24', 40, 120, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (49, 1, '2006-03-22 16:04:28', '2006-03-22 16:04:28', 41, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (50, 1, '2006-03-22 16:04:31', '2006-03-22 16:04:31', 48, 100, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (51, 1, '2006-03-22 16:04:38', '2006-03-22 16:04:38', 51, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (52, 1, '2006-03-22 16:04:41', '2006-03-22 16:04:41', 74, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (53, 1, '2006-03-22 16:04:45', '2006-03-22 16:04:45', 77, 60, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (54, 1, '2006-03-22 16:05:07', '2006-03-22 16:05:07', 3, 100, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (55, 1, '2006-03-22 16:05:11', '2006-03-22 16:05:11', 4, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (56, 1, '2006-03-22 16:05:14', '2006-03-22 16:05:14', 5, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (57, 1, '2006-03-22 16:05:26', '2006-03-22 16:05:26', 65, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (58, 1, '2006-03-22 16:05:32', '2006-03-22 16:05:32', 66, 80, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (59, 1, '2006-03-22 16:05:47', '2006-03-22 16:05:47', 1, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (60, 1, '2006-03-22 16:05:51', '2006-03-22 16:05:51', 34, 60, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (61, 1, '2006-03-22 16:06:00', '2006-03-22 16:06:00', 43, 100, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (62, 1, '2006-03-22 16:06:03', '2006-03-22 16:06:03', 81, 125, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (63, 2, '2006-03-22 16:07:56', '2006-03-24 11:03:00', 80, 30, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (64, 2, '2006-03-22 16:08:19', '2006-03-22 16:08:59', 7, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (65, 2, '2006-03-22 16:08:29', '2006-03-22 16:08:59', 51, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (66, 2, '2006-03-22 16:08:37', '2006-03-22 16:08:59', 80, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (67, 2, '2006-03-22 16:09:46', '2006-03-22 16:10:27', 1, 15, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (68, 2, '2006-03-22 16:10:06', '2006-03-22 16:10:27', 43, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (69, 2, '2006-03-22 16:11:39', '2006-03-24 11:00:55', 19, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (70, 2, '2006-03-22 16:11:56', '2006-03-24 10:59:41', 48, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (71, 2, '2006-03-22 16:12:29', '2006-03-24 10:57:38', 8, 17, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (72, 1, '2006-03-24 10:41:30', '2006-03-24 10:41:30', 81, 200, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (73, 2, '2006-03-24 10:41:33', '2006-03-24 10:41:42', 81, 200, NULL, NULL, 'Fill Back Ordered product, Order #40');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (74, 1, '2006-03-24 10:53:13', '2006-03-24 10:53:13', 48, 100, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (75, 2, '2006-03-24 10:53:16', '2006-03-24 10:55:46', 48, 100, NULL, NULL, 'Fill Back Ordered product, Order #39');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (76, 1, '2006-03-24 10:53:36', '2006-03-24 10:53:36', 43, 300, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (77, 2, '2006-03-24 10:53:39', '2006-03-24 10:56:57', 43, 300, NULL, NULL, 'Fill Back Ordered product, Order #38');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (78, 1, '2006-03-24 10:54:04', '2006-03-24 10:54:04', 41, 200, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (79, 2, '2006-03-24 10:54:07', '2006-03-24 10:58:40', 41, 200, NULL, NULL, 'Fill Back Ordered product, Order #36');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (80, 1, '2006-03-24 10:54:33', '2006-03-24 10:54:33', 19, 30, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (81, 2, '2006-03-24 10:54:35', '2006-03-24 11:02:02', 19, 30, NULL, NULL, 'Fill Back Ordered product, Order #33');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (82, 1, '2006-03-24 10:54:58', '2006-03-24 10:54:58', 34, 100, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (83, 2, '2006-03-24 10:55:02', '2006-03-24 11:03:00', 34, 100, NULL, NULL, 'Fill Back Ordered product, Order #30');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (84, 2, '2006-03-24 14:48:15', '2006-04-04 11:41:14', 6, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (85, 2, '2006-03-24 14:48:23', '2006-04-04 11:41:14', 4, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (86, 3, '2006-03-24 14:49:16', '2006-03-24 14:49:16', 80, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (87, 3, '2006-03-24 14:49:20', '2006-03-24 14:49:20', 81, 50, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (88, 3, '2006-03-24 14:50:09', '2006-03-24 14:50:09', 1, 25, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (89, 3, '2006-03-24 14:50:14', '2006-03-24 14:50:14', 43, 25, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (90, 3, '2006-03-24 14:50:18', '2006-03-24 14:50:18', 81, 25, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (91, 2, '2006-03-24 14:51:03', '2006-04-04 11:09:24', 40, 50, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (92, 2, '2006-03-24 14:55:03', '2006-04-04 11:06:56', 21, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (93, 2, '2006-03-24 14:55:39', '2006-04-04 11:06:13', 5, 25, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (94, 2, '2006-03-24 14:55:52', '2006-04-04 11:06:13', 41, 30, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (95, 2, '2006-03-24 14:56:09', '2006-04-04 11:06:13', 40, 30, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (96, 3, '2006-03-30 16:46:34', '2006-03-30 16:46:34', 34, 12, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (97, 3, '2006-03-30 17:23:27', '2006-03-30 17:23:27', 34, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (98, 3, '2006-03-30 17:24:33', '2006-03-30 17:24:33', 34, 1, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (99, 2, '2006-04-03 13:50:08', '2006-04-03 13:50:15', 48, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (100, 1, '2006-04-04 11:00:54', '2006-04-04 11:00:54', 57, 100, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (101, 2, '2006-04-04 11:00:56', '2006-04-04 11:08:49', 57, 100, NULL, NULL, 'Fill Back Ordered product, Order #46');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (102, 1, '2006-04-04 11:01:14', '2006-04-04 11:01:14', 34, 50, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (103, 1, '2006-04-04 11:01:35', '2006-04-04 11:01:35', 43, 250, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (104, 3, '2006-04-04 11:01:37', '2006-04-04 11:01:37', 43, 300, NULL, NULL, 'Fill Back Ordered product, Order #41');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (105, 1, '2006-04-04 11:01:55', '2006-04-04 11:01:55', 8, 25, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (106, 2, '2006-04-04 11:01:58', '2006-04-04 11:07:37', 8, 25, NULL, NULL, 'Fill Back Ordered product, Order #48');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (107, 1, '2006-04-04 11:02:17', '2006-04-04 11:02:17', 34, 300, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (108, 2, '2006-04-04 11:02:19', '2006-04-04 11:08:14', 34, 300, NULL, NULL, 'Fill Back Ordered product, Order #47');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (109, 1, '2006-04-04 11:02:37', '2006-04-04 11:02:37', 19, 25, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (110, 2, '2006-04-04 11:02:39', '2006-04-04 11:41:14', 19, 10, NULL, NULL, 'Fill Back Ordered product, Order #42');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (111, 1, '2006-04-04 11:02:56', '2006-04-04 11:02:56', 19, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (112, 2, '2006-04-04 11:02:58', '2006-04-04 11:07:37', 19, 25, NULL, NULL, 'Fill Back Ordered product, Order #48');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (113, 1, '2006-04-04 11:03:12', '2006-04-04 11:03:12', 72, 50, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (114, 2, '2006-04-04 11:03:14', '2006-04-04 11:08:49', 72, 50, NULL, NULL, 'Fill Back Ordered product, Order #46');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (115, 1, '2006-04-04 11:03:38', '2006-04-04 11:03:38', 41, 50, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (116, 2, '2006-04-04 11:03:39', '2006-04-04 11:09:24', 41, 50, NULL, NULL, 'Fill Back Ordered product, Order #45');
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (117, 2, '2006-04-04 11:04:55', '2006-04-04 11:05:04', 34, 87, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (118, 2, '2006-04-04 11:35:50', '2006-04-04 11:35:54', 51, 30, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (119, 2, '2006-04-04 11:35:51', '2006-04-04 11:35:54', 7, 30, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (120, 2, '2006-04-04 11:36:15', '2006-04-04 11:36:21', 17, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (121, 2, '2006-04-04 11:36:39', '2006-04-04 11:36:47', 6, 90, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (122, 2, '2006-04-04 11:37:06', '2006-04-04 11:37:09', 4, 30, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (123, 2, '2006-04-04 11:37:45', '2006-04-04 11:37:49', 48, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (124, 2, '2006-04-04 11:38:07', '2006-04-04 11:38:11', 48, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (125, 2, '2006-04-04 11:38:27', '2006-04-04 11:38:32', 41, 10, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (126, 2, '2006-04-04 11:38:48', '2006-04-04 11:38:53', 43, 5, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (127, 2, '2006-04-04 11:39:12', '2006-04-04 11:39:29', 40, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (128, 2, '2006-04-04 11:39:50', '2006-04-04 11:39:53', 8, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (129, 2, '2006-04-04 11:40:13', '2006-04-04 11:40:16', 80, 15, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (130, 2, '2006-04-04 11:40:32', '2006-04-04 11:40:38', 74, 20, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (131, 2, '2006-04-04 11:41:39', '2006-04-04 11:41:45', 72, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (132, 2, '2006-04-04 11:42:17', '2006-04-04 11:42:26', 3, 50, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (133, 2, '2006-04-04 11:42:24', '2006-04-04 11:42:26', 8, 3, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (134, 2, '2006-04-04 11:42:48', '2006-04-04 11:43:08', 20, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (135, 2, '2006-04-04 11:43:05', '2006-04-04 11:43:08', 52, 40, NULL, NULL, NULL);
INSERT INTO `inventory_transactions` (`id`, `transaction_type`, `transaction_created_date`, `transaction_modified_date`, `product_id`, `quantity`, `purchase_order_id`, `customer_order_id`, `comments`) VALUES (136, 3, '2006-04-25 17:04:05', '2006-04-25 17:04:57', 56, 110, NULL, NULL, NULL);
# 102 records

#
# Dumping data for table 'invoices'
#

INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (5, 31, '2006-03-22 16:08:59', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (6, 32, '2006-03-22 16:10:27', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (7, 40, '2006-03-24 10:41:41', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (8, 39, '2006-03-24 10:55:46', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (9, 38, '2006-03-24 10:56:57', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (10, 37, '2006-03-24 10:57:38', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (11, 36, '2006-03-24 10:58:40', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (12, 35, '2006-03-24 10:59:41', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (13, 34, '2006-03-24 11:00:55', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (14, 33, '2006-03-24 11:02:02', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (15, 30, '2006-03-24 11:03:00', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (16, 56, '2006-04-03 13:50:15', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (17, 55, '2006-04-04 11:05:04', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (18, 51, '2006-04-04 11:06:13', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (19, 50, '2006-04-04 11:06:56', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (20, 48, '2006-04-04 11:07:37', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (21, 47, '2006-04-04 11:08:14', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (22, 46, '2006-04-04 11:08:49', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (23, 45, '2006-04-04 11:09:24', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (24, 79, '2006-04-04 11:35:54', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (25, 78, '2006-04-04 11:36:21', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (26, 77, '2006-04-04 11:36:47', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (27, 76, '2006-04-04 11:37:09', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (28, 75, '2006-04-04 11:37:49', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (29, 74, '2006-04-04 11:38:11', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (30, 73, '2006-04-04 11:38:32', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (31, 72, '2006-04-04 11:38:53', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (32, 71, '2006-04-04 11:39:29', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (33, 70, '2006-04-04 11:39:53', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (34, 69, '2006-04-04 11:40:16', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (35, 67, '2006-04-04 11:40:38', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (36, 42, '2006-04-04 11:41:14', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (37, 60, '2006-04-04 11:41:45', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (38, 63, '2006-04-04 11:42:26', NULL, 0, 0, 0);
INSERT INTO `invoices` (`id`, `order_id`, `invoice_date`, `due_date`, `tax`, `shipping`, `amount_due`) VALUES (39, 58, '2006-04-04 11:43:08', NULL, 0, 0, 0);
# 35 records

#
# Dumping data for table 'order_details'
#

INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (27, 30, 34, 100, 14, 0, 2, NULL, 96, 83);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (28, 30, 80, 30, 3.5, 0, 2, NULL, NULL, 63);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (29, 31, 7, 10, 30, 0, 2, NULL, NULL, 64);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (30, 31, 51, 10, 53, 0, 2, NULL, NULL, 65);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (31, 31, 80, 10, 3.5, 0, 2, NULL, NULL, 66);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (32, 32, 1, 15, 18, 0, 2, NULL, NULL, 67);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (33, 32, 43, 20, 46, 0, 2, NULL, NULL, 68);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (34, 33, 19, 30, 9.2, 0, 2, NULL, 97, 81);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (35, 34, 19, 20, 9.2, 0, 2, NULL, NULL, 69);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (36, 35, 48, 10, 12.75, 0, 2, NULL, NULL, 70);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (37, 36, 41, 200, 9.65, 0, 2, NULL, 98, 79);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (38, 37, 8, 17, 40, 0, 2, NULL, NULL, 71);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (39, 38, 43, 300, 46, 0, 2, NULL, 99, 77);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (40, 39, 48, 100, 12.75, 0, 2, NULL, 100, 75);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (41, 40, 81, 200, 2.99, 0, 2, NULL, 101, 73);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (42, 41, 43, 300, 46, 0, 1, NULL, 102, 104);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (43, 42, 6, 10, 25, 0, 2, NULL, NULL, 84);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (44, 42, 4, 10, 22, 0, 2, NULL, NULL, 85);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (45, 42, 19, 10, 9.2, 0, 2, NULL, 103, 110);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (46, 43, 80, 20, 3.5, 0, 1, NULL, NULL, 86);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (47, 43, 81, 50, 2.99, 0, 1, NULL, NULL, 87);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (48, 44, 1, 25, 18, 0, 1, NULL, NULL, 88);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (49, 44, 43, 25, 46, 0, 1, NULL, NULL, 89);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (50, 44, 81, 25, 2.99, 0, 1, NULL, NULL, 90);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (51, 45, 41, 50, 9.65, 0, 2, NULL, 104, 116);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (52, 45, 40, 50, 18.4, 0, 2, NULL, NULL, 91);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (53, 46, 57, 100, 19.5, 0, 2, NULL, 105, 101);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (54, 46, 72, 50, 34.8, 0, 2, NULL, 106, 114);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (55, 47, 34, 300, 14, 0, 2, NULL, 107, 108);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (56, 48, 8, 25, 40, 0, 2, NULL, 108, 106);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (57, 48, 19, 25, 9.2, 0, 2, NULL, 109, 112);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (59, 50, 21, 20, 10, 0, 2, NULL, NULL, 92);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (60, 51, 5, 25, 21.35, 0, 2, NULL, NULL, 93);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (61, 51, 41, 30, 9.65, 0, 2, NULL, NULL, 94);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (62, 51, 40, 30, 18.4, 0, 2, NULL, NULL, 95);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (66, 56, 48, 10, 12.75, 0, 2, NULL, 111, 99);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (67, 55, 34, 87, 14, 0, 2, NULL, NULL, 117);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (68, 79, 7, 30, 30, 0, 2, NULL, NULL, 119);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (69, 79, 51, 30, 53, 0, 2, NULL, NULL, 118);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (70, 78, 17, 40, 39, 0, 2, NULL, NULL, 120);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (71, 77, 6, 90, 25, 0, 2, NULL, NULL, 121);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (72, 76, 4, 30, 22, 0, 2, NULL, NULL, 122);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (73, 75, 48, 40, 12.75, 0, 2, NULL, NULL, 123);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (74, 74, 48, 40, 12.75, 0, 2, NULL, NULL, 124);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (75, 73, 41, 10, 9.65, 0, 2, NULL, NULL, 125);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (76, 72, 43, 5, 46, 0, 2, NULL, NULL, 126);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (77, 71, 40, 40, 18.4, 0, 2, NULL, NULL, 127);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (78, 70, 8, 20, 40, 0, 2, NULL, NULL, 128);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (79, 69, 80, 15, 3.5, 0, 2, NULL, NULL, 129);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (80, 67, 74, 20, 10, 0, 2, NULL, NULL, 130);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (81, 60, 72, 40, 34.8, 0, 2, NULL, NULL, 131);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (82, 63, 3, 50, 10, 0, 2, NULL, NULL, 132);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (83, 63, 8, 3, 40, 0, 2, NULL, NULL, 133);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (84, 58, 20, 40, 81, 0, 2, NULL, NULL, 134);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (85, 58, 52, 40, 7, 0, 2, NULL, NULL, 135);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (86, 80, 56, 10, 38, 0, 1, NULL, NULL, 136);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (90, 81, 81, 0, 2.99, 0, 5, NULL, NULL, NULL);
INSERT INTO `order_details` (`id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`, `status_id`, `date_allocated`, `purchase_order_id`, `inventory_id`) VALUES (91, 81, 56, 0, 38, 0, 0, NULL, NULL, NULL);
# 58 records

#
# Dumping data for table 'order_details_status'
#

INSERT INTO `order_details_status` (`id`, `status_name`) VALUES (0, 'None');
INSERT INTO `order_details_status` (`id`, `status_name`) VALUES (1, 'Allocated');
INSERT INTO `order_details_status` (`id`, `status_name`) VALUES (2, 'Invoiced');
INSERT INTO `order_details_status` (`id`, `status_name`) VALUES (3, 'Shipped');
INSERT INTO `order_details_status` (`id`, `status_name`) VALUES (4, 'On Order');
INSERT INTO `order_details_status` (`id`, `status_name`) VALUES (5, 'No Stock');
# 6 records

#
# Dumping data for table 'orders'
#

INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (30, 9, 27, '2006-01-15 00:00:00', '2006-01-22 00:00:00', 2, 'Karen Toh', '789 27th Street', 'Las Vegas', 'NV', '99999', 'USA', 200, 0, 'Check', '2006-01-15 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (31, 3, 4, '2006-01-20 00:00:00', '2006-01-22 00:00:00', 1, 'Christina Lee', '123 4th Street', 'New York', 'NY', '99999', 'USA', 5, 0, 'Credit Card', '2006-01-20 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (32, 4, 12, '2006-01-22 00:00:00', '2006-01-22 00:00:00', 2, 'John Edwards', '123 12th Street', 'Las Vegas', 'NV', '99999', 'USA', 5, 0, 'Credit Card', '2006-01-22 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (33, 6, 8, '2006-01-30 00:00:00', '2006-01-31 00:00:00', 3, 'Elizabeth Andersen', '123 8th Street', 'Portland', 'OR', '99999', 'USA', 50, 0, 'Credit Card', '2006-01-30 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (34, 9, 4, '2006-02-06 00:00:00', '2006-02-07 00:00:00', 3, 'Christina Lee', '123 4th Street', 'New York', 'NY', '99999', 'USA', 4, 0, 'Check', '2006-02-06 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (35, 3, 29, '2006-02-10 00:00:00', '2006-02-12 00:00:00', 2, 'Soo Jung Lee', '789 29th Street', 'Denver', 'CO', '99999', 'USA', 7, 0, 'Check', '2006-02-10 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (36, 4, 3, '2006-02-23 00:00:00', '2006-02-25 00:00:00', 2, 'Thomas Axen', '123 3rd Street', 'Los Angelas', 'CA', '99999', 'USA', 7, 0, 'Cash', '2006-02-23 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (37, 8, 6, '2006-03-06 00:00:00', '2006-03-09 00:00:00', 2, 'Francisco Pérez-Olaeta', '123 6th Street', 'Milwaukee', 'WI', '99999', 'USA', 12, 0, 'Credit Card', '2006-03-06 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (38, 9, 28, '2006-03-10 00:00:00', '2006-03-11 00:00:00', 3, 'Amritansh Raghav', '789 28th Street', 'Memphis', 'TN', '99999', 'USA', 10, 0, 'Check', '2006-03-10 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (39, 3, 8, '2006-03-22 00:00:00', '2006-03-24 00:00:00', 3, 'Elizabeth Andersen', '123 8th Street', 'Portland', 'OR', '99999', 'USA', 5, 0, 'Check', '2006-03-22 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (40, 4, 10, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, 'Roland Wacker', '123 10th Street', 'Chicago', 'IL', '99999', 'USA', 9, 0, 'Credit Card', '2006-03-24 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (41, 1, 7, '2006-03-24 00:00:00', NULL, NULL, 'Ming-Yang Xie', '123 7th Street', 'Boise', 'ID', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (42, 1, 10, '2006-03-24 00:00:00', '2006-04-07 00:00:00', 1, 'Roland Wacker', '123 10th Street', 'Chicago', 'IL', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 2);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (43, 1, 11, '2006-03-24 00:00:00', NULL, 3, 'Peter Krschne', '123 11th Street', 'Miami', 'FL', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (44, 1, 1, '2006-03-24 00:00:00', NULL, NULL, 'Anna Bedecs', '123 1st Street', 'Seattle', 'WA', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (45, 1, 28, '2006-04-07 00:00:00', '2006-04-07 00:00:00', 3, 'Amritansh Raghav', '789 28th Street', 'Memphis', 'TN', '99999', 'USA', 40, 0, 'Credit Card', '2006-04-07 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (46, 7, 9, '2006-04-05 00:00:00', '2006-04-05 00:00:00', 1, 'Sven Mortensen', '123 9th Street', 'Salt Lake City', 'UT', '99999', 'USA', 100, 0, 'Check', '2006-04-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (47, 6, 6, '2006-04-08 00:00:00', '2006-04-08 00:00:00', 2, 'Francisco Pérez-Olaeta', '123 6th Street', 'Milwaukee', 'WI', '99999', 'USA', 300, 0, 'Credit Card', '2006-04-08 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (48, 4, 8, '2006-04-05 00:00:00', '2006-04-05 00:00:00', 2, 'Elizabeth Andersen', '123 8th Street', 'Portland', 'OR', '99999', 'USA', 50, 0, 'Check', '2006-04-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (50, 9, 25, '2006-04-05 00:00:00', '2006-04-05 00:00:00', 1, 'John Rodman', '789 25th Street', 'Chicago', 'IL', '99999', 'USA', 5, 0, 'Cash', '2006-04-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (51, 9, 26, '2006-04-05 00:00:00', '2006-04-05 00:00:00', 3, 'Run Liu', '789 26th Street', 'Miami', 'FL', '99999', 'USA', 60, 0, 'Credit Card', '2006-04-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (55, 1, 29, '2006-04-05 00:00:00', '2006-04-05 00:00:00', 2, 'Soo Jung Lee', '789 29th Street', 'Denver', 'CO', '99999', 'USA', 200, 0, 'Check', '2006-04-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (56, 2, 6, '2006-04-03 00:00:00', '2006-04-03 00:00:00', 3, 'Francisco Pérez-Olaeta', '123 6th Street', 'Milwaukee', 'WI', '99999', 'USA', 0, 0, 'Check', '2006-04-03 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (57, 9, 27, '2006-04-22 00:00:00', '2006-04-22 00:00:00', 2, 'Karen Toh', '789 27th Street', 'Las Vegas', 'NV', '99999', 'USA', 200, 0, 'Check', '2006-04-22 00:00:00', NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (58, 3, 4, '2006-04-22 00:00:00', '2006-04-22 00:00:00', 1, 'Christina Lee', '123 4th Street', 'New York', 'NY', '99999', 'USA', 5, 0, 'Credit Card', '2006-04-22 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (59, 4, 12, '2006-04-22 00:00:00', '2006-04-22 00:00:00', 2, 'John Edwards', '123 12th Street', 'Las Vegas', 'NV', '99999', 'USA', 5, 0, 'Credit Card', '2006-04-22 00:00:00', NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (60, 6, 8, '2006-04-30 00:00:00', '2006-04-30 00:00:00', 3, 'Elizabeth Andersen', '123 8th Street', 'Portland', 'OR', '99999', 'USA', 50, 0, 'Credit Card', '2006-04-30 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (61, 9, 4, '2006-04-07 00:00:00', '2006-04-07 00:00:00', 3, 'Christina Lee', '123 4th Street', 'New York', 'NY', '99999', 'USA', 4, 0, 'Check', '2006-04-07 00:00:00', NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (62, 3, 29, '2006-04-12 00:00:00', '2006-04-12 00:00:00', 2, 'Soo Jung Lee', '789 29th Street', 'Denver', 'CO', '99999', 'USA', 7, 0, 'Check', '2006-04-12 00:00:00', NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (63, 4, 3, '2006-04-25 00:00:00', '2006-04-25 00:00:00', 2, 'Thomas Axen', '123 3rd Street', 'Los Angelas', 'CA', '99999', 'USA', 7, 0, 'Cash', '2006-04-25 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (64, 8, 6, '2006-05-09 00:00:00', '2006-05-09 00:00:00', 2, 'Francisco Pérez-Olaeta', '123 6th Street', 'Milwaukee', 'WI', '99999', 'USA', 12, 0, 'Credit Card', '2006-05-09 00:00:00', NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (65, 9, 28, '2006-05-11 00:00:00', '2006-05-11 00:00:00', 3, 'Amritansh Raghav', '789 28th Street', 'Memphis', 'TN', '99999', 'USA', 10, 0, 'Check', '2006-05-11 00:00:00', NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (66, 3, 8, '2006-05-24 00:00:00', '2006-05-24 00:00:00', 3, 'Elizabeth Andersen', '123 8th Street', 'Portland', 'OR', '99999', 'USA', 5, 0, 'Check', '2006-05-24 00:00:00', NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (67, 4, 10, '2006-05-24 00:00:00', '2006-05-24 00:00:00', 2, 'Roland Wacker', '123 10th Street', 'Chicago', 'IL', '99999', 'USA', 9, 0, 'Credit Card', '2006-05-24 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (68, 1, 7, '2006-05-24 00:00:00', NULL, NULL, 'Ming-Yang Xie', '123 7th Street', 'Boise', 'ID', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (69, 1, 10, '2006-05-24 00:00:00', NULL, 1, 'Roland Wacker', '123 10th Street', 'Chicago', 'IL', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (70, 1, 11, '2006-05-24 00:00:00', NULL, 3, 'Peter Krschne', '123 11th Street', 'Miami', 'FL', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (71, 1, 1, '2006-05-24 00:00:00', NULL, 3, 'Anna Bedecs', '123 1st Street', 'Seattle', 'WA', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (72, 1, 28, '2006-06-07 00:00:00', '2006-06-07 00:00:00', 3, 'Amritansh Raghav', '789 28th Street', 'Memphis', 'TN', '99999', 'USA', 40, 0, 'Credit Card', '2006-06-07 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (73, 7, 9, '2006-06-05 00:00:00', '2006-06-05 00:00:00', 1, 'Sven Mortensen', '123 9th Street', 'Salt Lake City', 'UT', '99999', 'USA', 100, 0, 'Check', '2006-06-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (74, 6, 6, '2006-06-08 00:00:00', '2006-06-08 00:00:00', 2, 'Francisco Pérez-Olaeta', '123 6th Street', 'Milwaukee', 'WI', '99999', 'USA', 300, 0, 'Credit Card', '2006-06-08 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (75, 4, 8, '2006-06-05 00:00:00', '2006-06-05 00:00:00', 2, 'Elizabeth Andersen', '123 8th Street', 'Portland', 'OR', '99999', 'USA', 50, 0, 'Check', '2006-06-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (76, 9, 25, '2006-06-05 00:00:00', '2006-06-05 00:00:00', 1, 'John Rodman', '789 25th Street', 'Chicago', 'IL', '99999', 'USA', 5, 0, 'Cash', '2006-06-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (77, 9, 26, '2006-06-05 00:00:00', '2006-06-05 00:00:00', 3, 'Run Liu', '789 26th Street', 'Miami', 'FL', '99999', 'USA', 60, 0, 'Credit Card', '2006-06-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (78, 1, 29, '2006-06-05 00:00:00', '2006-06-05 00:00:00', 2, 'Soo Jung Lee', '789 29th Street', 'Denver', 'CO', '99999', 'USA', 200, 0, 'Check', '2006-06-05 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (79, 2, 6, '2006-06-23 00:00:00', '2006-06-23 00:00:00', 3, 'Francisco Pérez-Olaeta', '123 6th Street', 'Milwaukee', 'WI', '99999', 'USA', 0, 0, 'Check', '2006-06-23 00:00:00', NULL, 0, NULL, 3);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (80, 2, 4, '2006-04-25 17:03:55', NULL, NULL, 'Christina Lee', '123 4th Street', 'New York', 'NY', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
INSERT INTO `orders` (`id`, `employee_id`, `customer_id`, `order_date`, `shipped_date`, `shipper_id`, `ship_name`, `ship_address`, `ship_city`, `ship_state_province`, `ship_zip_postal_code`, `ship_country_region`, `shipping_fee`, `taxes`, `payment_type`, `paid_date`, `notes`, `tax_rate`, `tax_status_id`, `status_id`) VALUES (81, 2, 3, '2006-04-25 17:26:53', NULL, NULL, 'Thomas Axen', '123 3rd Street', 'Los Angelas', 'CA', '99999', 'USA', 0, 0, NULL, NULL, NULL, 0, NULL, 0);
# 48 records

#
# Dumping data for table 'orders_status'
#

INSERT INTO `orders_status` (`id`, `status_name`) VALUES (0, 'New');
INSERT INTO `orders_status` (`id`, `status_name`) VALUES (1, 'Invoiced');
INSERT INTO `orders_status` (`id`, `status_name`) VALUES (2, 'Shipped');
INSERT INTO `orders_status` (`id`, `status_name`) VALUES (3, 'Closed');
# 4 records

#
# Dumping data for table 'orders_tax_status'
#

INSERT INTO `orders_tax_status` (`id`, `tax_status_name`) VALUES (0, 'Tax Exempt');
INSERT INTO `orders_tax_status` (`id`, `tax_status_name`) VALUES (1, 'Taxable');
# 2 records

#
# Dumping data for table 'privileges'
#

INSERT INTO `privileges` (`id`, `privilege_name`) VALUES (2, 'Purchase Approvals');
# 1 records

#
# Dumping data for table 'products'
#

INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('4', 1, 'NWTB-1', 'Northwind Traders Chai', NULL, 13.5, 18, 10, 40, '10 boxes x 20 bags', 0, 10, 'Beverages', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('10', 3, 'NWTCO-3', 'Northwind Traders Syrup', NULL, 7.5, 10, 25, 100, '12 - 550 ml bottles', 0, 25, 'Condiments', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('10', 4, 'NWTCO-4', 'Northwind Traders Cajun Seasoning', NULL, 16.5, 22, 10, 40, '48 - 6 oz jars', 0, 10, 'Condiments', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('10', 5, 'NWTO-5', 'Northwind Traders Olive Oil', NULL, 16.0125, 21.35, 10, 40, '36 boxes', 0, 10, 'Oil', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('2;6', 6, 'NWTJP-6', 'Northwind Traders Boysenberry Spread', NULL, 18.75, 25, 25, 100, '12 - 8 oz jars', 0, 25, 'Jams, Preserves', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('2', 7, 'NWTDFN-7', 'Northwind Traders Dried Pears', NULL, 22.5, 30, 10, 40, '12 - 1 lb pkgs.', 0, 10, 'Dried Fruit & Nuts', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('8', 8, 'NWTS-8', 'Northwind Traders Curry Sauce', NULL, 30, 40, 10, 40, '12 - 12 oz jars', 0, 10, 'Sauces', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('2;6', 14, 'NWTDFN-14', 'Northwind Traders Walnuts', NULL, 17.4375, 23.25, 10, 40, '40 - 100 g pkgs.', 0, 10, 'Dried Fruit & Nuts', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 17, 'NWTCFV-17', 'Northwind Traders Fruit Cocktail', NULL, 29.25, 39, 10, 40, '15.25 OZ', 0, 10, 'Canned Fruit & Vegetables', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 19, 'NWTBGM-19', 'Northwind Traders Chocolate Biscuits Mix', NULL, 6.9, 9.2, 5, 20, '10 boxes x 12 pieces', 0, 5, 'Baked Goods & Mixes', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('2;6', 20, 'NWTJP-6', 'Northwind Traders Marmalade', NULL, 60.75, 81, 10, 40, '30 gift boxes', 0, 10, 'Jams, Preserves', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 21, 'NWTBGM-21', 'Northwind Traders Scones', NULL, 7.5, 10, 5, 20, '24 pkgs. x 4 pieces', 0, 5, 'Baked Goods & Mixes', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('4', 34, 'NWTB-34', 'Northwind Traders Beer', NULL, 10.5, 14, 15, 60, '24 - 12 oz bottles', 0, 15, 'Beverages', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('7', 40, 'NWTCM-40', 'Northwind Traders Crab Meat', NULL, 13.8, 18.4, 30, 120, '24 - 4 oz tins', 0, 30, 'Canned Meat', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 41, 'NWTSO-41', 'Northwind Traders Clam Chowder', NULL, 7.2375, 9.65, 10, 40, '12 - 12 oz cans', 0, 10, 'Soups', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('3;4', 43, 'NWTB-43', 'Northwind Traders Coffee', NULL, 34.5, 46, 25, 100, '16 - 500 g tins', 0, 25, 'Beverages', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('10', 48, 'NWTCA-48', 'Northwind Traders Chocolate', NULL, 9.5625, 12.75, 25, 100, '10 pkgs', 0, 25, 'Candy', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('2', 51, 'NWTDFN-51', 'Northwind Traders Dried Apples', NULL, 39.75, 53, 10, 40, '50 - 300 g pkgs.', 0, 10, 'Dried Fruit & Nuts', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 52, 'NWTG-52', 'Northwind Traders Long Grain Rice', NULL, 5.25, 7, 25, 100, '16 - 2 kg boxes', 0, 25, 'Grains', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 56, 'NWTP-56', 'Northwind Traders Gnocchi', NULL, 28.5, 38, 30, 120, '24 - 250 g pkgs.', 0, 30, 'Pasta', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 57, 'NWTP-57', 'Northwind Traders Ravioli', NULL, 14.625, 19.5, 20, 80, '24 - 250 g pkgs.', 0, 20, 'Pasta', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('8', 65, 'NWTS-65', 'Northwind Traders Hot Pepper Sauce', NULL, 15.7875, 21.05, 10, 40, '32 - 8 oz bottles', 0, 10, 'Sauces', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('8', 66, 'NWTS-66', 'Northwind Traders Tomato Sauce', NULL, 12.75, 17, 20, 80, '24 - 8 oz jars', 0, 20, 'Sauces', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('5', 72, 'NWTD-72', 'Northwind Traders Mozzarella', NULL, 26.1, 34.8, 10, 40, '24 - 200 g pkgs.', 0, 10, 'Dairy products', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('2;6', 74, 'NWTDFN-74', 'Northwind Traders Almonds', NULL, 7.5, 10, 5, 20, '5 kg pkg.', 0, 5, 'Dried Fruit & Nuts', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('10', 77, 'NWTCO-77', 'Northwind Traders Mustard', NULL, 9.75, 13, 15, 60, '12 boxes', 0, 15, 'Condiments', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('2', 80, 'NWTDFN-80', 'Northwind Traders Dried Plums', NULL, 3, 3.5, 50, 75, '1 lb bag', 0, 25, 'Dried Fruit & Nuts', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('3', 81, 'NWTB-81', 'Northwind Traders Green Tea', NULL, 2, 2.99, 100, 125, '20 bags per box', 0, 25, 'Beverages', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 82, 'NWTC-82', 'Northwind Traders Granola', NULL, 2, 4, 20, 100, NULL, 0, NULL, 'Cereal', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('9', 83, 'NWTCS-83', 'Northwind Traders Potato Chips', NULL, .5, 1.8, 30, 200, NULL, 0, NULL, 'Chips, Snacks', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 85, 'NWTBGM-85', 'Northwind Traders Brownie Mix', NULL, 9, 12.49, 10, 20, '3 boxes', 0, 5, 'Baked Goods & Mixes', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 86, 'NWTBGM-86', 'Northwind Traders Cake Mix', NULL, 10.5, 15.99, 10, 20, '4 boxes', 0, 5, 'Baked Goods & Mixes', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('7', 87, 'NWTB-87', 'Northwind Traders Tea', NULL, 2, 4, 20, 50, '100 count per box', 0, NULL, 'Beverages', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 88, 'NWTCFV-88', 'Northwind Traders Pears', NULL, 1, 1.3, 10, 40, '15.25 OZ', 0, NULL, 'Canned Fruit & Vegetables', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 89, 'NWTCFV-89', 'Northwind Traders Peaches', NULL, 1, 1.5, 10, 40, '15.25 OZ', 0, NULL, 'Canned Fruit & Vegetables', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 90, 'NWTCFV-90', 'Northwind Traders Pineapple', NULL, 1, 1.8, 10, 40, '15.25 OZ', 0, NULL, 'Canned Fruit & Vegetables', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 91, 'NWTCFV-91', 'Northwind Traders Cherry Pie Filling', NULL, 1, 2, 10, 40, '15.25 OZ', 0, NULL, 'Canned Fruit & Vegetables', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 92, 'NWTCFV-92', 'Northwind Traders Green Beans', NULL, 1, 1.2, 10, 40, '14.5 OZ', 0, NULL, 'Canned Fruit & Vegetables', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 93, 'NWTCFV-93', 'Northwind Traders Corn', NULL, 1, 1.2, 10, 40, '14.5 OZ', 0, NULL, 'Canned Fruit & Vegetables', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 94, 'NWTCFV-94', 'Northwind Traders Peas', NULL, 1, 1.5, 10, 40, '14.5 OZ', 0, NULL, 'Canned Fruit & Vegetables', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('7', 95, 'NWTCM-95', 'Northwind Traders Tuna Fish', NULL, .5, 2, 30, 50, '5 oz', 0, NULL, 'Canned Meat', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('7', 96, 'NWTCM-96', 'Northwind Traders Smoked Salmon', NULL, 2, 4, 30, 50, '5 oz', 0, NULL, 'Canned Meat', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('1', 97, 'NWTC-82', 'Northwind Traders Hot Cereal', NULL, 3, 5, 50, 200, NULL, 0, NULL, 'Cereal', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 98, 'NWTSO-98', 'Northwind Traders Vegetable Soup', NULL, 1, 1.89, 100, 200, NULL, 0, NULL, 'Soups', '');
INSERT INTO `products` (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`) VALUES ('6', 99, 'NWTSO-99', 'Northwind Traders Chicken Soup', NULL, 1, 1.95, 100, 200, NULL, 0, NULL, 'Soups', '');
# 45 records

#
# Dumping data for table 'purchase_order_details'
#

INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (238, 90, 1, 40, 14, '2006-01-22 00:00:00', 1, 59);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (239, 91, 3, 100, 8, '2006-01-22 00:00:00', 1, 54);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (240, 91, 4, 40, 16, '2006-01-22 00:00:00', 1, 55);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (241, 91, 5, 40, 16, '2006-01-22 00:00:00', 1, 56);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (242, 92, 6, 100, 19, '2006-01-22 00:00:00', 1, 40);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (243, 92, 7, 40, 22, '2006-01-22 00:00:00', 1, 41);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (244, 92, 8, 40, 30, '2006-01-22 00:00:00', 1, 42);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (245, 92, 14, 40, 17, '2006-01-22 00:00:00', 1, 43);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (246, 92, 17, 40, 29, '2006-01-22 00:00:00', 1, 44);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (247, 92, 19, 20, 7, '2006-01-22 00:00:00', 1, 45);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (248, 92, 20, 40, 61, '2006-01-22 00:00:00', 1, 46);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (249, 92, 21, 20, 8, '2006-01-22 00:00:00', 1, 47);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (250, 90, 34, 60, 10, '2006-01-22 00:00:00', 1, 60);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (251, 92, 40, 120, 14, '2006-01-22 00:00:00', 1, 48);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (252, 92, 41, 40, 7, '2006-01-22 00:00:00', 1, 49);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (253, 90, 43, 100, 34, '2006-01-22 00:00:00', 1, 61);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (254, 92, 48, 100, 10, '2006-01-22 00:00:00', 1, 50);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (255, 92, 51, 40, 40, '2006-01-22 00:00:00', 1, 51);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (256, 93, 52, 100, 5, '2006-01-22 00:00:00', 1, 37);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (257, 93, 56, 120, 28, '2006-01-22 00:00:00', 1, 38);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (258, 93, 57, 80, 15, '2006-01-22 00:00:00', 1, 39);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (259, 91, 65, 40, 16, '2006-01-22 00:00:00', 1, 57);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (260, 91, 66, 80, 13, '2006-01-22 00:00:00', 1, 58);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (261, 94, 72, 40, 26, '2006-01-22 00:00:00', 1, 36);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (262, 92, 74, 20, 8, '2006-01-22 00:00:00', 1, 52);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (263, 92, 77, 60, 10, '2006-01-22 00:00:00', 1, 53);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (264, 95, 80, 75, 3, '2006-01-22 00:00:00', 1, 35);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (265, 90, 81, 125, 2, '2006-01-22 00:00:00', 1, 62);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (266, 96, 34, 100, 10, '2006-01-22 00:00:00', 1, 82);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (267, 97, 19, 30, 7, '2006-01-22 00:00:00', 1, 80);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (268, 98, 41, 200, 7, '2006-01-22 00:00:00', 1, 78);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (269, 99, 43, 300, 34, '2006-01-22 00:00:00', 1, 76);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (270, 100, 48, 100, 10, '2006-01-22 00:00:00', 1, 74);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (271, 101, 81, 200, 2, '2006-01-22 00:00:00', 1, 72);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (272, 102, 43, 300, 34, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (273, 103, 19, 10, 7, '2006-04-17 00:00:00', 1, 111);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (274, 104, 41, 50, 7, '2006-04-06 00:00:00', 1, 115);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (275, 105, 57, 100, 15, '2006-04-05 00:00:00', 1, 100);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (276, 106, 72, 50, 26, '2006-04-05 00:00:00', 1, 113);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (277, 107, 34, 300, 10, '2006-04-05 00:00:00', 1, 107);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (278, 108, 8, 25, 30, '2006-04-05 00:00:00', 1, 105);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (279, 109, 19, 25, 7, '2006-04-05 00:00:00', 1, 109);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (280, 110, 43, 250, 34, '2006-04-10 00:00:00', 1, 103);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (281, 90, 1, 40, 14, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (282, 92, 19, 20, 7, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (283, 111, 34, 50, 10, '2006-04-04 00:00:00', 1, 102);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (285, 91, 3, 50, 8, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (286, 91, 4, 40, 16, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (288, 140, 85, 10, 9, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (289, 141, 6, 10, 18.75, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (290, 142, 1, 1, 13.5, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (292, 146, 20, 40, 60, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (293, 146, 51, 40, 39, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (294, 147, 40, 120, 13, NULL, 0, NULL);
INSERT INTO `purchase_order_details` (`id`, `purchase_order_id`, `product_id`, `quantity`, `unit_cost`, `date_received`, `posted_to_inventory`, `inventory_id`) VALUES (295, 148, 72, 40, 26, NULL, 0, NULL);
# 55 records

#
# Dumping data for table 'purchase_order_status'
#

INSERT INTO `purchase_order_status` (`id`, `status`) VALUES (0, 'New');
INSERT INTO `purchase_order_status` (`id`, `status`) VALUES (1, 'Submitted');
INSERT INTO `purchase_order_status` (`id`, `status`) VALUES (2, 'Approved');
INSERT INTO `purchase_order_status` (`id`, `status`) VALUES (3, 'Closed');
# 4 records

#
# Dumping data for table 'purchase_orders'
#

INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (90, 1, 2, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, NULL, 2, '2006-01-22 00:00:00', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (91, 3, 2, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, NULL, 2, '2006-01-22 00:00:00', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (92, 2, 2, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, NULL, 2, '2006-01-22 00:00:00', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (93, 5, 2, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, NULL, 2, '2006-01-22 00:00:00', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (94, 6, 2, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, NULL, 2, '2006-01-22 00:00:00', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (95, 4, 2, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, NULL, 2, '2006-01-22 00:00:00', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (96, 1, 5, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #30', 2, '2006-01-22 00:00:00', 5);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (97, 2, 7, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #33', 2, '2006-01-22 00:00:00', 7);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (98, 2, 4, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #36', 2, '2006-01-22 00:00:00', 4);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (99, 1, 3, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #38', 2, '2006-01-22 00:00:00', 3);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (100, 2, 9, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #39', 2, '2006-01-22 00:00:00', 9);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (101, 1, 2, '2006-01-14 00:00:00', '2006-01-22 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #40', 2, '2006-01-22 00:00:00', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (102, 1, 1, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #41', 2, '2006-04-04 00:00:00', 1);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (103, 2, 1, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #42', 2, '2006-04-04 00:00:00', 1);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (104, 2, 1, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #45', 2, '2006-04-04 00:00:00', 1);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (105, 5, 7, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, 'Check', 'Purchase generated based on Order #46', 2, '2006-04-04 00:00:00', 7);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (106, 6, 7, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #46', 2, '2006-04-04 00:00:00', 7);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (107, 1, 6, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #47', 2, '2006-04-04 00:00:00', 6);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (108, 2, 4, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #48', 2, '2006-04-04 00:00:00', 4);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (109, 2, 4, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #48', 2, '2006-04-04 00:00:00', 4);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (110, 1, 3, '2006-03-24 00:00:00', '2006-03-24 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #49', 2, '2006-04-04 00:00:00', 3);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (111, 1, 2, '2006-03-31 00:00:00', '2006-03-31 00:00:00', 2, NULL, 0, 0, NULL, 0, NULL, 'Purchase generated based on Order #56', 2, '2006-04-04 00:00:00', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (140, 6, NULL, '2006-04-25 00:00:00', '2006-04-25 16:40:51', 2, NULL, 0, 0, NULL, 0, NULL, NULL, 2, '2006-04-25 16:41:33', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (141, 8, NULL, '2006-04-25 00:00:00', '2006-04-25 17:10:35', 2, NULL, 0, 0, NULL, 0, NULL, NULL, 2, '2006-04-25 17:10:55', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (142, 8, NULL, '2006-04-25 00:00:00', '2006-04-25 17:18:29', 2, NULL, 0, 0, NULL, 0, 'Check', NULL, 2, '2006-04-25 17:18:51', 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (146, 2, 2, '2006-04-26 18:26:37', '2006-04-26 18:26:37', 1, NULL, 0, 0, NULL, 0, NULL, NULL, NULL, NULL, 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (147, 7, 2, '2006-04-26 18:33:28', '2006-04-26 18:33:28', 1, NULL, 0, 0, NULL, 0, NULL, NULL, NULL, NULL, 2);
INSERT INTO `purchase_orders` (`id`, `supplier_id`, `created_by`, `submitted_date`, `creation_date`, `status_id`, `expected_date`, `shipping_fee`, `taxes`, `payment_date`, `payment_amount`, `payment_method`, `notes`, `approved_by`, `approved_date`, `submitted_by`) VALUES (148, 5, 2, '2006-04-26 18:33:52', '2006-04-26 18:33:52', 1, NULL, 0, 0, NULL, 0, NULL, NULL, NULL, NULL, 2);
# 28 records

#
# Dumping data for table 'sales_reports'
#

INSERT INTO `sales_reports` (`group_by`, `display`, `title`, `filter_row_source`, `default`) VALUES ('Category', 'Category', 'Sales By Category', 'SELECT DISTINCT [Category] FROM [products] ORDER BY [Category];', 0);
INSERT INTO `sales_reports` (`group_by`, `display`, `title`, `filter_row_source`, `default`) VALUES ('country_region', 'Country/Region', 'Sales By Country', 'SELECT DISTINCT [country_region] FROM [customers Extended] ORDER BY [country_region];', 0);
INSERT INTO `sales_reports` (`group_by`, `display`, `title`, `filter_row_source`, `default`) VALUES ('Customer ID', 'Customer', 'Sales By Customer', 'SELECT DISTINCT [Company] FROM [customers Extended] ORDER BY [Company];', 0);
INSERT INTO `sales_reports` (`group_by`, `display`, `title`, `filter_row_source`, `default`) VALUES ('employee_id', 'Employee', 'Sales By Employee', 'SELECT DISTINCT [Employee Name] FROM [employees Extended] ORDER BY [Employee Name];', 0);
INSERT INTO `sales_reports` (`group_by`, `display`, `title`, `filter_row_source`, `default`) VALUES ('Product ID', 'Product', 'Sales by Product', 'SELECT DISTINCT [Product Name] FROM [products] ORDER BY [Product Name];', 1);
# 5 records

#
# Dumping data for table 'shippers'
#

INSERT INTO `shippers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (1, 'Shipping Company A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '123 Any Street', 'Memphis', 'TN', '99999', 'USA', NULL, NULL, '');
INSERT INTO `shippers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (2, 'Shipping Company B', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '123 Any Street', 'Memphis', 'TN', '99999', 'USA', NULL, NULL, '');
INSERT INTO `shippers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (3, 'Shipping Company C', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '123 Any Street', 'Memphis', 'TN', '99999', 'USA', NULL, NULL, '');
# 3 records

#
# Dumping data for table 'strings'
#

INSERT INTO `strings` (`string_id`, `string_data`) VALUES (2, 'Northwind Traders');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (3, 'Cannot remove posted inventory!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (4, 'Back ordered product filled for Order #|');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (5, 'Discounted price below cost!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (6, 'Insufficient inventory.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (7, 'Insufficient inventory. Do you want to create a purchase order?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (8, 'Purchase orders were successfully created for | products');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (9, 'There are no products below their respective reorder levels');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (10, 'Must specify customer name!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (11, 'Restocking will generate purchase orders for all products below desired inventory levels.  Do you want to continue?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (12, 'Cannot create purchase order.  No suppliers listed for specified product');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (13, 'Discounted price is below cost!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (14, 'Do you want to continue?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (15, 'Order is already invoiced. Do you want to print the invoice?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (16, 'Order does not contain any line items');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (17, 'Cannot create invoice!  Inventory has not been allocated for each specified product.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (18, 'Sorry, there are no sales in the specified time period');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (19, 'Product successfully restocked.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (21, 'Product does not need restocking! Product is already at desired inventory level.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (22, 'Product restocking failed!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (23, 'Invalid login specified!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (24, 'Must first select reported!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (25, 'Changing supplier will remove purchase line items, continue?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (26, 'Purchase orders were successfully submitted for | products.  Do you want to view the restocking report?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (27, 'There was an error attempting to restock inventory levels.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (28, '| product(s) were successfully restocked.  Do you want to view the restocking report?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (29, 'You cannot remove purchase line items already posted to inventory!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (30, 'There was an error removing one or more purchase line items.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (31, 'You cannot modify quantity for purchased product already received or posted to inventory.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (32, 'You cannot modify price for purchased product already received or posted to inventory.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (33, 'Product has been successfully posted to inventory.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (34, 'Sorry, product cannot be successfully posted to inventory.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (35, 'There are orders with this product on back order.  Would you like to fill them now?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (36, 'Cannot post product to inventory without specifying received date!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (37, 'Do you want to post received product to inventory?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (38, 'Initialize purchase, orders, and inventory data?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (39, 'Must first specify employee name!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (40, 'Specified user must be logged in to approve purchase!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (41, 'Purchase order must contain completed line items before it can be approved');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (42, 'Sorry, you do not have permission to approve purchases.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (43, 'Purchase successfully approved');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (44, 'Purchase cannot be approved');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (45, 'Purchase successfully submitted for approval');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (46, 'Purchase cannot be submitted for approval');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (47, 'Sorry, purchase order does not contain line items');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (48, 'Do you want to cancel this order?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (49, 'Canceling an order will permanently delete the order.  Are you sure you want to cancel?');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (100, 'Your order was successfully canceled.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (101, 'Cannot cancel an order that has items received and posted to inventory.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (102, 'There was an error trying to cancel this order.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (103, 'The invoice for this order has not yet been created.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (104, 'Shipping information is not complete.  Please specify all shipping information and try again.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (105, 'Cannot mark as shipped.  Order must first be invoiced!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (106, 'Cannot cancel an order that has already shipped!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (107, 'Must first specify salesperson!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (108, 'Order is now marked closed.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (109, 'Order must first be marked shipped before closing.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (110, 'Must first specify payment information!');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (111, 'There was an error attempting to restock inventory levels.  | product(s) were successfully restocked.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (112, 'You must supply a Unit Cost.');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (113, 'Fill back ordered product, Order #|');
INSERT INTO `strings` (`string_id`, `string_data`) VALUES (114, 'Purchase generated based on Order #|');
# 62 records

#
# Dumping data for table 'suppliers'
#

INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (1, 'Supplier A', 'Andersen', 'Elizabeth A.', NULL, 'Sales Manager', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (2, 'Supplier B', 'Weiler', 'Cornelia', NULL, 'Sales Manager', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (3, 'Supplier C', 'Kelley', 'Madeleine', NULL, 'Sales Representative', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (4, 'Supplier D', 'Sato', 'Naoki', NULL, 'Marketing Manager', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (5, 'Supplier E', 'Hernandez-Echevarria', 'Amaya', NULL, 'Sales Manager', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (6, 'Supplier F', 'Hayakawa', 'Satomi', NULL, 'Marketing Assistant', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (7, 'Supplier G', 'Glasson', 'Stuart', NULL, 'Marketing Manager', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (8, 'Supplier H', 'Dunton', 'Bryn Paul', NULL, 'Sales Representative', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (9, 'Supplier I', 'Sandberg', 'Mikael', NULL, 'Sales Manager', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
INSERT INTO `suppliers` (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`) VALUES (10, 'Supplier J', 'Sousa', 'Luis', NULL, 'Sales Manager', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '');
# 10 records

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;