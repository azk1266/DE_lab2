SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema f1_qlf_db
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema f1_qlf_db
-- -----------------------------------------------------
DROP SCHEMA `f1_qlf_db`;
CREATE SCHEMA IF NOT EXISTS `f1_qlf_db` DEFAULT CHARACTER SET utf8 ;
SHOW WARNINGS;
USE `f1_qlf_db` ;

-- -----------------------------------------------------
-- Table `f1_qlf_db`.`dim_circuit`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_qlf_db`.`dim_circuit` ;

SHOW WARNINGS;
-- from data/circuits.csv
CREATE TABLE IF NOT EXISTS `dim_circuit` (
  `circuit_key` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(100) NOT NULL,
  `city` VARCHAR(80) NULL,
  `country` VARCHAR(80) NULL,
  `latitude` DECIMAL(9,6) NULL,
  `longitude` DECIMAL(9,6) NULL,
  `altitude_m` SMALLINT NULL
) ENGINE=InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `f1_qlf_db`.`dim_constructor`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_qlf_db`.`dim_constructor` ;

SHOW WARNINGS;
/* from data/constructors.csv */
CREATE TABLE IF NOT EXISTS `f1_qlf_db`.`dim_constructor` (
  `constructor_key` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(100) NOT NULL,
  `nationality` VARCHAR(50) NULL
) ENGINE=InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `f1_qlf_db`.`dim_driver`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_qlf_db`.`dim_driver` ;

SHOW WARNINGS;
/* from data/drivers.csv */
CREATE TABLE IF NOT EXISTS `f1_qlf_db`.`dim_driver` (
  `driver_key` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(100) NOT NULL,
  `nationality` VARCHAR(50) NULL,
  `birthdate` DATE NULL,
  `country` VARCHAR(80) NULL
) ENGINE=InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `f1_qlf_db`.`dim_date`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_qlf_db`.`dim_date` ;

SHOW WARNINGS;
 /* The date of qualification session equal to corresponding race date minus 1. 
 The date of the relevant race must be taken from data/races.csv. */
CREATE TABLE IF NOT EXISTS `f1_qlf_db`.`dim_date` (
  `date_key` INT PRIMARY KEY, /* race date */
  `year` SMALLINT NOT NULL,
  `month` TINYINT NOT NULL,
  `day_of_month` TINYINT NOT NULL,
  `day_name` VARCHAR(10) NULL,
  `day_of_week` TINYINT NULL,
  `time_bucket` ENUM('morning','afternoon','evening') NULL
) ENGINE=InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `f1_qlf_db`.`facts`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_qlf_db`.`facts` ;

SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `f1_qlf_db`.`facts` (
  `qualifying_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `dim_circuit_circuit_key` INT NOT NULL,
  `dim_constructor_constructor_key` INT NOT NULL,
  `dim_driver_driver_key` INT NOT NULL,
  `dim_date_date_key` INT NOT NULL,
  `position` TINYINT NULL,
  `q1_ms` MEDIUMINT NULL, /* in milliseconds */
  `q2_ms` MEDIUMINT NULL,
  `q3_ms` MEDIUMINT NULL,
  /* status — Derived field indicating the driver's result in the qualifying session.
     It is not present in the original dataset, but calculated from the availability of q1, q2, and q3 times:
     'OK' — the driver set times in all qualifying rounds (Q1–Q3).
     'DNQ' — the driver participated in Q1 but did not advance to Q2.
     'DNS' — the driver did not start the qualifying session (no times recorded).
     'DSQ' — the driver was disqualified or had no valid times due to penalties.
     This column helps categorize the performance outcome of each driver during qualification. */
  `status` ENUM('OK', 'DNQ', 'DSQ', 'DNS') NULL,
  CONSTRAINT `fk_facts_dim_circuit`
    FOREIGN KEY (`dim_circuit_circuit_key`)
    REFERENCES `f1_qlf_db`.`dim_circuit` (`circuit_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_facts_dim_constructor1`
    FOREIGN KEY (`dim_constructor_constructor_key`)
    REFERENCES `f1_qlf_db`.`dim_constructor` (`constructor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_facts_dim_driver1`
    FOREIGN KEY (`dim_driver_driver_key`)
    REFERENCES `f1_qlf_db`.`dim_driver` (`driver_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_facts_dim_date1`
    FOREIGN KEY (`dim_date_date_key`)
    REFERENCES `f1_qlf_db`.`dim_date` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

SHOW WARNINGS;
CREATE INDEX `fk_facts_dim_circuit_idx` ON `f1_qlf_db`.`facts` (`dim_circuit_circuit_key` ASC) VISIBLE;

SHOW WARNINGS;
CREATE INDEX `fk_facts_dim_constructor1_idx` ON `f1_qlf_db`.`facts` (`dim_constructor_constructor_key` ASC) VISIBLE;

SHOW WARNINGS;
CREATE INDEX `fk_facts_dim_driver1_idx` ON `f1_qlf_db`.`facts` (`dim_driver_driver_key` ASC) VISIBLE;

SHOW WARNINGS;
CREATE INDEX `fk_facts_dim_date1_idx` ON `f1_qlf_db`.`facts` (`dim_date_date_key` ASC) VISIBLE;

SHOW WARNINGS;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
