/*
 * Filename: /home/john/projects/DeepCTR/deepctr/data/database.sql
 * Path: /home/john/projects/DeepCTR/notes
 * Created Date: Saturday, May 21st 2022, 4:38:17 am
 * Author: John James
 *
 * Copyright (c) 2022 John James
 */

CREATE DATABASE IF NOT EXISTS deepctr;
USE deepctr;

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
    `created` DATETIME(6) NOT NULL,
    `modified` DATETIME(6) NOT NULL,
    `accessed` DATETIME(6) NOT NULL,
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
    `created` DATETIME(6) NOT NULL,
    `modified` DATETIME(6) NOT NULL,
    `accessed` DATETIME(6) NOT NULL,
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
    `created` DATETIME(6) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

CREATE TABLE `dag` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(256) NULL,
    `n_tasks` INTEGER NOT NULL,
    `n_tasks_done` INTEGER NOT NULL,
    `started` DATETIME(6) NULL,
    `stopped` DATETIME(6) NULL,
    `duration` BIGINT NULL,
    `return_code` INTEGER NOT NULL,
    `created` DATETIME(6) NOT NULL,
    `executed` DATETIME(6) NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

CREATE TABLE `task` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(256) NULL,
    `seq` INTEGER NULL,
    `dag_id` INTEGER NULL,
    `started` DATETIME(6) NULL,
    `stopped` DATETIME(6) NULL,
    `duration` BIGINT NULL,
    `return_code` INTEGER NOT NULL,
    `created` DATETIME(6) NOT NULL,
    `modified` DATETIME(6) NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

SET FOREIGN_KEY_CHECKS = 1;