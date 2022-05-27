/*
 * Filename: /home/john/projects/DeepCTR/tests/test_data/test_db_setup.sql
 * Path: /home/john/projects/DeepCTR/notes
 * Created Date: Saturday, May 21st 2022, 4:38:17 am
 * Author: John James
 *
 * Copyright (c) 2022 John James
 */
DROP TABLE IF EXISTS testtable;
DROP DATABASE IF EXISTS testdb;

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS testtable (
        `id` INTEGER NOT NULL AUTO_INCREMENT,
        `number` INTEGER NOT NULL,
        `letters` VARCHAR(32) NOT NULL,
        PRIMARY KEY (`id`)
    )  ENGINE=InnoDB;
ALTER TABLE `testtable` AUTO_INCREMENT=1;
GRANT ALL PRIVILEGES ON testdb TO 'john'@'localhost' WITH GRANT OPTION;