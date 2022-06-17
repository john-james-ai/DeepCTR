/*
 * Filename: /home/john/projects/DeepCTR/frameworks/mysql/dataset.sql
 * Path: /home/john/projects/DeepCTR/notes
 * Created Date: Saturday, May 21st 2022, 4:38:17 am
 * Author: John James
 *
 * Copyright (c) 2022 John James
 */

CREATE DATABASE IF NOT EXISTS deepctr;
USE deepctr;

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS `localfile`;
DROP TABLE IF EXISTS `localdataset`;
DROP TABLE IF EXISTS `s3file`;
DROP TABLE IF EXISTS `s3dataset`;
DROP TABLE IF EXISTS `dag`;
DROP TABLE IF EXISTS `tasks`;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE `localdataset` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `datasource` VARCHAR(32) NOT NULL,
    `stage` VARCHAR(16) NOT NULL,
    `storage_type` VARCHAR(16) NOT NULL,
    `folder` VARCHAR(64) NOT NULL,
    `size` BIGINT NULL,
    `dag_id` INTEGER NULL,
    `home` VARCHAR(64) NOT NULL,
    `created` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`),
    INDEX `idx` (`datasource`, `name`, `stage`),
    CONSTRAINT `dataset_key` UNIQUE(`datasource`,`name`,`stage`)
) ENGINE=InnoDB;

CREATE TABLE `localfile` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL,
    `dataset` VARCHAR(32) NOT NULL,
    `dataset_id` INTEGER NOT NULL,
    `datasource` VARCHAR(32) NOT NULL,
    `stage` VARCHAR(16) NOT NULL,
    `storage_type` VARCHAR(16) NOT NULL,
    `filename` VARCHAR(128) NOT NULL,
    `filepath` VARCHAR(256) NULL,
    `format` VARCHAR(24) NOT NULL,
    `compressed` BOOLEAN NOT NULL,
    `size` BIGINT NULL,
    `dag_id` INTEGER NULL,
    `task_id` INTEGER NULL,
    `home` VARCHAR(64) NOT NULL,
    `created` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    CONSTRAINT `file_key` UNIQUE(`dataset_id`,`name`)
) ENGINE=InnoDB;


CREATE TABLE `s3dataset` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `datasource` VARCHAR(32) NOT NULL,
    `storage_type` VARCHAR(16) NOT NULL,
    `bucket` VARCHAR(32) NOT NULL,
    `folder` VARCHAR(256) NOT NULL,
    `size` BIGINT NULL,
    `dag_id` INTEGER NULL,
    `created` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`),
    INDEX `idx` (`datasource`, `name`),
    CONSTRAINT `dataset_key` UNIQUE(`datasource`,`name`)
) ENGINE=InnoDB;

CREATE TABLE `s3file` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL,
    `dataset` VARCHAR(32) NOT NULL,
    `dataset_id` INTEGER NOT NULL,
    `datasource` VARCHAR(32) NOT NULL,
    `storage_type` VARCHAR(16) NOT NULL,
    `bucket` VARCHAR(32) NOT NULL,
    `object_key` VARCHAR(256) NOT NULL,
    `format` VARCHAR(24) NOT NULL,
    `compressed` BOOLEAN NOT NULL,
    `size` BIGINT NULL,
    `dag_id` INTEGER NULL,
    `task_id` INTEGER NULL,
    `created` DATETIME NOT NULL,
    PRIMARY KEY (`id`),
    CONSTRAINT `file_key` UNIQUE(`dataset_id`,`name`)
) ENGINE=InnoDB;



CREATE TABLE `dag` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(256) NULL,
    `start` DATETIME NULL,
    `end` DATETIME NULL,
    `duration` BIGINT NULL,
    `created` DATETIME NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

CREATE TABLE `task` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL,
    `desc` VARCHAR(256) NULL,
    `task_no` INTEGER NULL,
    `dag_id` INTEGER NULL,
    `start` DATETIME NULL,
    `end` DATETIME NULL,
    `duration` BIGINT NULL,
    `created` DATETIME NULL,
    PRIMARY KEY (`id`),
    UNIQUE (`id`)
) ENGINE=InnoDB;

ALTER TABLE `localfile` ADD FOREIGN KEY (`dataset_id`) REFERENCES `localdataset`(`id`);
ALTER TABLE `localfile` ADD FOREIGN KEY (`task_id`) REFERENCES `task`(`id`);
ALTER TABLE `localfile` ADD FOREIGN KEY (`dag_id`) REFERENCES `dag`(`id`);
ALTER TABLE `localdataset` ADD FOREIGN KEY (`dag_id`) REFERENCES `dag`(`id`);

ALTER TABLE `s3file` ADD FOREIGN KEY (`dataset_id`) REFERENCES `s3dataset`(`id`);
ALTER TABLE `s3file` ADD FOREIGN KEY (`task_id`) REFERENCES `task`(`id`);
ALTER TABLE `s3file` ADD FOREIGN KEY (`dag_id`) REFERENCES `dag`(`id`);
ALTER TABLE `s3dataset` ADD FOREIGN KEY (`dag_id`) REFERENCES `dag`(`id`);

ALTER TABLE `task` ADD FOREIGN KEY (`dag_id`) REFERENCES `dag`(`id`);

