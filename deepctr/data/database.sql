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
DROP TABLE IF EXISTS `dataset`;
DROP TABLE IF EXISTS `localfile`;
DROP TABLE IF EXISTS `s3file`;
DROP TABLE IF EXISTS `localdataset`;
DROP TABLE IF EXISTS `s3dataset`;
DROP TABLE IF EXISTS `dag`;
DROP TABLE IF EXISTS `task`;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE `localfile` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `source` VARCHAR(32) NOT NULL,
    `dataset` VARCHAR(32) NOT NULL,
    `stage_id` INTEGER, NOT NULL,
    `stage_name` VARCHAR(16) NOT NULL,
    `filepath` VARCHAR(256) NOT NULL,
    `folder` VARCHAR(256) NOT NULL,
    `filename` VARCHAR(256) NOT NULL,
    `format` VARCHAR(16) NOT NULL,
    `compressed` BOOLEAN NOT NULL,
    `size` BIGINT NULL,
    `dag_id` INTEGER NULL,
    `task_id` INTEGER NULL,
    `created` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`),
    INDEX `idx` (`source`, `name`, `entity`),
    CONSTRAINT `dataset_key` UNIQUE(`source`,`name`,`entity`)
) ENGINE=InnoDB;


CREATE TABLE `s3file` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `source` VARCHAR(32) NOT NULL,
    `dataset` VARCHAR(32) NOT NULL,
    `stage_id` INTEGER, NOT NULL,
    `stage_name` VARCHAR(16) NOT NULL,
    `bucket` VARCHAR(32) NOT NULL,
    `object_key` VARCHAR(256) NOT NULL,
    `filename` VARCHAR(256) NOT NULL,
    `format` VARCHAR(16) NOT NULL,
    `compressed` BOOLEAN NOT NULL,
    `size` BIGINT NULL,
    `dag_id` INTEGER NULL,
    `task_id` INTEGER NULL,
    `created` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`),
    INDEX `idx` (`source`, `name`, `entity`),
    CONSTRAINT `dataset_key` UNIQUE(`source`,`name`, `entity`)
) ENGINE=InnoDB;

CREATE TABLE `dag` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(256) NULL,
    `start` DATETIME NULL,
    `stop` DATETIME NULL,
    `duration` BIGINT NULL,
    `created` DATETIME NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

CREATE TABLE `task` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(256) NULL,
    `seq` INTEGER NULL,
    `dag_id` INTEGER NULL,
    `start` DATETIME NULL,
    `stop` DATETIME NULL,
    `duration` BIGINT NULL,
    `created` DATETIME NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;


