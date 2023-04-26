-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `mydb` DEFAULT CHARACTER SET utf8 ;
USE `mydb` ;

-- -----------------------------------------------------
-- Table `mydb`.`fechas`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`fechas` (
  `idfechas` INT NOT NULL,
  `fecha` DATE NULL,
  PRIMARY KEY (`idfechas`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `mydb`.`comunidades`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`comunidades` (
  `idcomunidades` INT NOT NULL,
  `comunidad` VARCHAR(45) NULL,
  PRIMARY KEY (`idcomunidades`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `mydb`.`nacional_renovable_no_renovable`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`nacional_renovable_no_renovable` (
  `idnacional_renovable_no_renovable` INT NOT NULL,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  PRIMARY KEY (`idnacional_renovable_no_renovable`),
  INDEX `fk_nacional_renovable_no_renovable_fechas_idx` (`fechas_idfechas` ASC) VISIBLE,
  CONSTRAINT `fk_nacional_renovable_no_renovable_fechas`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `mydb`.`fechas` (`idfechas`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `mydb`.`comunidades_renovables_no_renovables`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`comunidades_renovables_no_renovables` (
  `idcomunidades_renovables_no_renovables` INT NOT NULL,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  `comunidades_idcomunidades` INT NOT NULL,
  PRIMARY KEY (`idcomunidades_renovables_no_renovables`),
  INDEX `fk_comunidades_renovables_no_renovables_fechas1_idx` (`fechas_idfechas` ASC) VISIBLE,
  INDEX `fk_comunidades_renovables_no_renovables_comunidades1_idx` (`comunidades_idcomunidades` ASC) VISIBLE,
  CONSTRAINT `fk_comunidades_renovables_no_renovables_fechas1`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `mydb`.`fechas` (`idfechas`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_comunidades_renovables_no_renovables_comunidades1`
    FOREIGN KEY (`comunidades_idcomunidades`)
    REFERENCES `mydb`.`comunidades` (`idcomunidades`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
