/*
 * Filename: /home/john/projects/DeepCTR/tests/test_dal/test_db_teardown.sql
 * Path: /home/john/projects/DeepCTR/notes
 * Created Date: Saturday, May 21st 2022, 4:38:17 am
 * Author: John James
 *
 * Copyright (c) 2022 John James
 */
USE testdb;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS localfile;
DROP TABLE IF EXISTS S3file;
DROP TABLE IF EXISTS localdataset;
DROP TABLE IF EXISTS s3dataset;
DROP TABLE IF EXISTS dag;
DROP TABLE IF EXISTS task;
DROP DATABASE IF EXISTS testdb;
SET FOREIGN_KEY_CHECKS = 1;