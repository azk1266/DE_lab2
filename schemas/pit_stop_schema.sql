SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema f1_pit_db
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema f1_pit_db
-- ----------------------------------------------------- 
DROP SCHEMA  `f1_pit_db`;
CREATE SCHEMA IF NOT EXISTS `f1_pit_db` DEFAULT CHARACTER SET utf8;
SHOW WARNINGS;
USE `f1_pit_db`;

-- -----------------------------------------------------
-- Table `f1_pit_db`.`dim_circuit`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_pit_db`.`dim_circuit`;

SHOW WARNINGS;
-- from data/circuits.csv
CREATE TABLE IF NOT EXISTS `f1_pit_db`.`dim_circuit` (
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
-- Table `f1_pit_db`.`dim_constructor`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_pit_db`.`dim_constructor`;

SHOW WARNINGS;
/* from data/constructors.csv */
CREATE TABLE IF NOT EXISTS `f1_pit_db`.`dim_constructor` (
  `constructor_key` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(100) NOT NULL,
  `nationality` VARCHAR(50) NULL
) ENGINE=InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `f1_pit_db`.`dim_driver`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_pit_db`.`dim_driver`;

SHOW WARNINGS;
/* from data/drivers.csv */
CREATE TABLE IF NOT EXISTS `f1_pit_db`.`dim_driver` (
  `driver_key` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(100) NOT NULL,
  `nationality` VARCHAR(50) NULL,
  `birthdate` DATE NULL,
  `country` VARCHAR(80) NULL
) ENGINE=InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `f1_pit_db`.`dim_date`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_pit_db`.`dim_date`;
/* from data/races.csv */
SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `f1_pit_db`.`dim_date` (
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
-- Table `f1_pit_db`.`dim_race`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_pit_db`.`dim_race`;

SHOW WARNINGS;
/*from data/races.csv*/
CREATE TABLE IF NOT EXISTS `f1_pit_db`.`dim_race` (
  `race_key`     INT AUTO_INCREMENT PRIMARY KEY,                  
  `year`         SMALLINT NOT NULL,
  `round`        SMALLINT NOT NULL,
  `name`         VARCHAR(120) NOT NULL,          /* Grand Prix name */   
  `race_date_key` INT NOT NULL,                 /* YYYYMMDD(races.date)*/
  `start_time_key` INT NULL                    /* HHMMSS(races.time)*/
) ENGINE=InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `f1_pit_db`.`facts`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_pit_db`.`fact_pit_stop`;

SHOW WARNINGS;

CREATE TABLE IF NOT EXISTS `f1_pit_db`.`fact_pit_stop` (
  `pit_stop_key` BIGINT AUTO_INCREMENT PRIMARY KEY,   /* surrogate key for the fact row */
  `dim_race_race_key` INT NOT NULL,
  `dim_circuit_circuit_key` INT NOT NULL,
  `dim_constructor_constructor_key` INT NOT NULL,
  `dim_driver_driver_key` INT NOT NULL,
  `dim_date_date_key` INT NOT NULL,
  `stop_number` TINYINT NOT NULL,       /* pit_stops.stop */
  `lap` SMALLINT NOT NULL,              /* pit_stops.lap */
  `duration_ms` INT NOT NULL,     /* pit_stops.milliseconds */

  /* Indexes and constraints */
  CONSTRAINT `fk_fps_race`
    FOREIGN KEY (`dim_race_race_key`)
    REFERENCES `f1_pit_db`.`dim_race` (`race_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,

  CONSTRAINT `fk_fps_circuit`
    FOREIGN KEY (`dim_circuit_circuit_key`)
    REFERENCES `f1_pit_db`.`dim_circuit` (`circuit_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,

  CONSTRAINT `fk_fps_constructor`
    FOREIGN KEY (`dim_constructor_constructor_key`)
    REFERENCES `f1_pit_db`.`dim_constructor` (`constructor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,

  CONSTRAINT `fk_fps_driver`
    FOREIGN KEY (`dim_driver_driver_key`)
    REFERENCES `f1_pit_db`.`dim_driver` (`driver_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,

  CONSTRAINT `fk_fps_date`
    FOREIGN KEY (`dim_date_date_key`)
    REFERENCES `f1_pit_db`.`dim_date` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
) ENGINE=InnoDB;

SHOW WARNINGS;
CREATE INDEX `idx_fps_race`        ON `f1_pit_db`.`fact_pit_stop` (`dim_race_race_key`);

SHOW WARNINGS;
CREATE INDEX `idx_fps_circuit`     ON `f1_pit_db`.`fact_pit_stop` (`dim_circuit_circuit_key`);

SHOW WARNINGS;
CREATE INDEX `idx_fps_constructor` ON `f1_pit_db`.`fact_pit_stop` (`dim_constructor_constructor_key`);

SHOW WARNINGS;
CREATE INDEX `idx_fps_driver`      ON `f1_pit_db`.`fact_pit_stop` (`dim_driver_driver_key`);

SHOW WARNINGS;
CREATE INDEX `idx_fps_date`        ON `f1_pit_db`.`fact_pit_stop` (`dim_date_date_key`);

SHOW WARNINGS;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
