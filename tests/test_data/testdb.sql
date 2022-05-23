/*
 * Filename: /home/john/projects/DeepCTR/tests/test_data/testdb.sql
 * Path: /home/john/projects/DeepCTR/notes
 * Created Date: Saturday, May 21st 2022, 4:38:17 am
 * Author: John James
 *
 * Copyright (c) 2022 John James
 */
USE testdb;
DROP TABLE testtable;
DROP DATABASE testdb;

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS testtable (
        `id` INTEGER NOT NULL AUTO_INCREMENT,
        `number` INTEGER NOT NULL,
        `letters` VARCHAR(32) NOT NULL,
        PRIMARY KEY (`id`)
    )  ENGINE=InnoDB;

GRANT ALL PRIVILEGES ON testdb TO 'john'@'localhost' WITH GRANT OPTION;