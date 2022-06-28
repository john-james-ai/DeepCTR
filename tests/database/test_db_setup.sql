/*
 * Filename: /home/john/projects/DeepCTR/tests/database/test_db_setup.sql
 * Path: /home/john/projects/DeepCTR/notes
 * Created Date: Saturday, May 21st 2022, 4:38:17 am
 * Author: John James
 *
 * Copyright (c) 2022 John James
 */

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS `file`;
DROP TABLE IF EXISTS `fileset`;
DROP TABLE IF EXISTS `dataset`;
DROP TABLE IF EXISTS `localfile`;
DROP TABLE IF EXISTS `source`;
DROP TABLE IF EXISTS `s3file`;
DROP TABLE IF EXISTS `localdataset`;
DROP TABLE IF EXISTS `s3dataset`;
DROP TABLE IF EXISTS `dag`;
DROP TABLE IF EXISTS `task`;


CREATE TABLE `source` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(128) NOT NULL,
    `url` VARCHAR(256) NOT NULL,
    `created` DATETIME NOT NULL,
    `modified` DATETIME NOT NULL,
    `accessed` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;


CREATE TABLE `file` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(128) NOT NULL,
    `folder` VARCHAR(128) NOT NULL,
    `format` VARCHAR(16) NOT NULL,
    `filename` VARCHAR(64) NOT NULL,
    `filepath` VARCHAR(256) NOT NULL,
    `compressed` BOOLEAN NOT NULL,
    `size` BIGINT NULL,
    `file_created` DATETIME NOT NULL,
    `file_modified` DATETIME NOT NULL,
    `file_accessed` DATETIME NOT NULL,
    `created` DATETIME NOT NULL,
    `modified` DATETIME NOT NULL,
    `accessed` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;


CREATE TABLE `dataset` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `source` VARCHAR(32) NOT NULL,
    `file_system` VARCHAR(8) NOT NULL,
    `stage_id` INTEGER NOT NULL,
    `stage_name` VARCHAR(16) NOT NULL,
    `home` VARCHAR(64) NOT NULL,
    `bucket` VARCHAR(32) NULL,
    `folder` VARCHAR(256) NOT NULL,
    `format` VARCHAR(16) NOT NULL,
    `compressed` BOOLEAN NOT NULL,
    `size` BIGINT NOT NULL,
    `created` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

CREATE TABLE `dag` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(256) NULL,
    `n_tasks` INTEGER NOT NULL,
    `n_tasks_done` INTEGER NOT NULL,
    `started` DATETIME NULL,
    `stopped` DATETIME NULL,
    `duration` BIGINT NULL,
    `return_code` INTEGER NOT NULL,
    `created` DATETIME NOT NULL,
    `executed` DATETIME NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

CREATE TABLE `task` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(256) NULL,
    `seq` INTEGER NULL,
    `dag_id` INTEGER NULL,
    `started` DATETIME NULL,
    `stopped` DATETIME NULL,
    `duration` BIGINT NULL,
    `return_code` INTEGER NOT NULL,
    `created` DATETIME NOT NULL,
    `modified` DATETIME NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

SET FOREIGN_KEY_CHECKS = 1;
GRANT ALL PRIVILEGES ON testdal TO 'john'@'localhost' WITH GRANT OPTION;
