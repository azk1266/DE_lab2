SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema f1_res_db
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema f1_res_db
-- -----------------------------------------------------
DROP SCHEMA `f1_res_db`;
CREATE SCHEMA IF NOT EXISTS `f1_res_db` DEFAULT CHARACTER SET utf8;
SHOW WARNINGS;
USE `f1_res_db`;

-- -----------------------------------------------------
-- dim_circuit (from circuits.csv via races.circuitId)
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_res_db`.`dim_circuit`;
SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `f1_res_db`.`dim_circuit` (
  `circuit_key` INT AUTO_INCREMENT PRIMARY KEY,
  `name`       VARCHAR(100) NOT NULL,
  `city`       VARCHAR(80)  NULL,
  `country`    VARCHAR(80)  NULL,
  `latitude`   DECIMAL(9,6) NULL,
  `longitude`  DECIMAL(9,6) NULL,
  `altitude_m` SMALLINT     NULL
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- dim_constructor (from constructors.csv)
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_res_db`.`dim_constructor`;
SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `f1_res_db`.`dim_constructor` (
  `constructor_key` INT AUTO_INCREMENT PRIMARY KEY,
  `name`        VARCHAR(100) NOT NULL,
  `nationality` VARCHAR(50)  NULL
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- dim_driver (from drivers.csv)
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_res_db`.`dim_driver`;
SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `f1_res_db`.`dim_driver` (
  `driver_key`  INT AUTO_INCREMENT PRIMARY KEY,
  `name`        VARCHAR(100) NOT NULL,
  `nationality` VARCHAR(50)  NULL,
  `birthdate`   DATE         NULL,
  `country`     VARCHAR(80)  NULL
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- dim_date (smart key = YYYYMMDD from races.date)
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_res_db`.`dim_date`;
SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `f1_res_db`.`dim_date` (
  `date_key`     INT PRIMARY KEY,
  `year`         SMALLINT NOT NULL,
  `month`        TINYINT  NOT NULL,
  `day_of_month` TINYINT  NOT NULL,
  `day_name`     VARCHAR(10) NULL,
  `day_of_week`  TINYINT  NULL,
  `time_bucket`  ENUM('morning','afternoon','evening') NULL
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- dim_race (from races.csv)
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_res_db`.`dim_race`;
SHOW WARNINGS;
CREATE TABLE  IF NOT EXISTS `f1_res_db`.`dim_race` (
  `race_key`       INT AUTO_INCREMENT PRIMARY KEY,
  `year`           SMALLINT     NOT NULL,
  `round`          SMALLINT     NOT NULL,
  `name`           VARCHAR(120) NOT NULL,  /* Grand Prix name */
  `race_date_key`  INT          NOT NULL,  /* YYYYMMDD(races.date) */
  `start_time_key` INT          NULL       /* HHMMSS(races.time) as integer */
) ENGINE=InnoDB;


-- -----------------------------------------------------
-- fact_race_result (from results.csv)
-- -----------------------------------------------------
DROP TABLE IF EXISTS `f1_res_db`.`fact_race_result`;
SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `f1_res_db`.`fact_race_result` (
  `race_result_key` BIGINT AUTO_INCREMENT PRIMARY KEY,
  /* Foreign keys to dimensions */
  `dim_race_race_key`              INT NOT NULL,
  `dim_circuit_circuit_key`        INT NOT NULL,
  `dim_constructor_constructor_key` INT NOT NULL,
  `dim_driver_driver_key`          INT NOT NULL,
  `dim_date_date_key`              INT NOT NULL,
  /* Measures and attributes */
  `grid`             TINYINT NULL,              /* starting grid position */
  `finish_position`  TINYINT NULL,              /* final numeric position */
  `position_text`    VARCHAR(8) NULL,           /* e.g. '1','R','DNF' */
  `position_order`   TINYINT NULL,
  `points`           DECIMAL(5,2) NULL,
  `laps`             SMALLINT NULL,
  `race_time_ms`     INT NULL,                  /* total race time in ms */
  `fastest_lap_no`   SMALLINT NULL,
  `fastest_lap_rank` TINYINT NULL,
  `fastest_lap_time_ms` INT NULL,
  `fastest_lap_speed` DECIMAL(6,3) NULL,
  `status_text`      VARCHAR(50) NULL,          /* from status.csv via statusId */

  /* Foreign key constraints */
  CONSTRAINT `fk_frr_race`
    FOREIGN KEY (`dim_race_race_key`)
    REFERENCES `f1_res_db`.`dim_race` (`race_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,

  CONSTRAINT `fk_frr_circuit`
    FOREIGN KEY (`dim_circuit_circuit_key`)
    REFERENCES `f1_res_db`.`dim_circuit` (`circuit_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,

  CONSTRAINT `fk_frr_constructor`
    FOREIGN KEY (`dim_constructor_constructor_key`)
    REFERENCES `f1_res_db`.`dim_constructor` (`constructor_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,

  CONSTRAINT `fk_frr_driver`
    FOREIGN KEY (`dim_driver_driver_key`)
    REFERENCES `f1_res_db`.`dim_driver` (`driver_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,

  CONSTRAINT `fk_frr_date`
    FOREIGN KEY (`dim_date_date_key`)
    REFERENCES `f1_res_db`.`dim_date` (`date_key`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
) ENGINE=InnoDB;

SHOW WARNINGS;

-- Indexes for joins and filtering
SHOW WARNINGS;
CREATE INDEX `idx_frr_race`        ON `f1_res_db`.`fact_race_result` (`dim_race_race_key`);
SHOW WARNINGS;
CREATE INDEX `idx_frr_circuit`     ON `f1_res_db`.`fact_race_result` (`dim_circuit_circuit_key`);
SHOW WARNINGS;
CREATE INDEX `idx_frr_constructor` ON `f1_res_db`.`fact_race_result` (`dim_constructor_constructor_key`);
SHOW WARNINGS;
CREATE INDEX `idx_frr_driver`      ON `f1_res_db`.`fact_race_result` (`dim_driver_driver_key`);
SHOW WARNINGS;
CREATE INDEX `idx_frr_date`        ON `f1_res_db`.`fact_race_result` (`dim_date_date_key`);
SHOW WARNINGS;
CREATE INDEX `idx_frr_status_txt`  ON `f1_res_db`.`fact_race_result` (`status_text`);
SHOW WARNINGS;

SHOW WARNINGS;

CREATE UNIQUE INDEX `ux_frr_race_driver`
  ON `f1_res_db`.`fact_race_result` (`dim_race_race_key`,`dim_driver_driver_key`);

SHOW WARNINGS;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;