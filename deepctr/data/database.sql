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


CREATE TABLE `file` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `source` VARCHAR(32) NOT NULL,
    `dataset_id` INTEGER NOT NULL,
    `dataset` VARCHAR(32) NOT NULL,
    `storage_type` VARCHAR(8) NOT NULL,
    `format` VARCHAR(16) NOT NULL,
    `stage_id` INTEGER NOT NULL,
    `stage_name` VARCHAR(16) NOT NULL,
    `home` VARCHAR(64) NOT NULL,
    `bucket` VARCHAR(32) NULL,
    `filepath` VARCHAR(256) NOT NULL,
    `compressed` BOOLEAN NOT NULL,
    `size` BIGINT NULL,
    `rows` BIGINT NULL,
    `cols` BIGINT NULL,
    `exists` BOOLEAN NOT NULL,
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
    `storage_type` VARCHAR(8) NOT NULL,
    `stage_id` INTEGER NOT NULL,
    `stage_name` VARCHAR(16) NOT NULL,
    `home` VARCHAR(64) NOT NULL,
    `bucket` VARCHAR(32) NULL,
    `folder` VARCHAR(256) NOT NULL,
    `format` VARCHAR(16) NOT NULL,
    `size` BIGINT NOT NULL,
    `created` DATETIME NOT NULL,
    `modified` DATETIME NOT NULL,
    `accessed` DATETIME NOT NULL,
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

ALTER TABLE `file` ADD FOREIGN KEY (`dataset`) REFERENCES `dataset`(`name`);
SET FOREIGN_KEY_CHECKS = 1;