#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_file.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday June 28th 2022 12:18:55 pm                                                  #
# Modified   : Tuesday June 28th 2022 08:28:41 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import logging
import logging.config
import pyspark
from datetime import datetime
from copy import deepcopy

from deepctr.dal.file import File
from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.dao import DAO

# Enter imports for modules and classes being tested here

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                       TEST FILE                                                  #
# ================================================================================================ #
@pytest.mark.dal
@pytest.mark.file
@pytest.mark.pfile
class TestParquetFile:
    def compare_dates_accessed(self, a, b):
        assert a.created == b.created
        assert a.modified == b.modified
        assert a.accessed != b.accessed

    def compare_dates_updated(self, a, b):
        assert a.created == b.created
        assert a.modified != b.modified
        assert a.accessed != b.accessed

    def test_create_parquet(self, caplog, parquet_file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        assert parquet_file.name == "test_parquet_file"
        assert parquet_file.desc == "Test Parquet File"
        assert parquet_file.folder == "tests/data/data_store"
        assert parquet_file.format == "parquet"
        assert parquet_file.filename == "parquetfile.parquet"
        assert parquet_file.compressed is False
        assert parquet_file.filepath == os.path.join(parquet_file.folder, parquet_file.filename)
        assert parquet_file.size != 0
        assert isinstance(parquet_file.created, datetime)
        assert isinstance(parquet_file.modified, datetime)
        assert isinstance(parquet_file.accessed, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read_parquet(self, caplog, parquet_file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        file = deepcopy(parquet_file)
        data = file.read()
        assert isinstance(data, pyspark.sql.DataFrame)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_write_parquet(self, caplog, parquet_file, spark_dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        file = deepcopy(parquet_file)
        file.delete()
        assert not os.path.exists(file.filepath)

        file.write(spark_dataframe)
        assert os.path.exists(file.filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_exists_parquet(self, caplog, parquet_file):

        file = deepcopy(parquet_file)
        assert file.exists()


@pytest.mark.dal
@pytest.mark.file
@pytest.mark.csvfile
class TestCSVFile:
    def compare_dates_accessed(self, a, b):
        assert a.created == b.created
        assert a.modified == b.modified
        assert a.accessed != b.accessed

    def compare_dates_updated(self, a, b):
        assert a.created == b.created
        assert a.modified != b.modified
        assert a.accessed != b.accessed

    def test_create_csv(self, caplog, csv_file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        assert csv_file.name == "test_csv_file"
        assert csv_file.desc == "Test CSV File"
        assert csv_file.folder == "tests/data/data_store"
        assert csv_file.format == "csv"
        assert csv_file.filename == "csvfile.csv"
        assert csv_file.compressed is False
        assert csv_file.filepath == os.path.join(csv_file.folder, csv_file.filename)
        assert csv_file.size != 0
        assert isinstance(csv_file.created, datetime)
        assert isinstance(csv_file.modified, datetime)
        assert isinstance(csv_file.accessed, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read_csv(self, caplog, csv_file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        file = deepcopy(csv_file)
        data = file.read()
        assert isinstance(data, pyspark.sql.DataFrame)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_write_csv(self, caplog, csv_file, spark_dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        file = deepcopy(csv_file)
        file.delete()
        assert not os.path.exists(file.filepath)

        file.write(spark_dataframe)
        assert os.path.exists(file.filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_exists_csv(self, caplog, csv_file):

        file = deepcopy(csv_file)
        assert file.exists()


@pytest.mark.dal
@pytest.mark.file
@pytest.mark.filedao
class TestFileDAO:
    def create_file(self, i: int):
        file = File(
            name="test_file_{}".format(str(i)),
            desc="Test File {}".format(str(i)),
            format="csv",
            folder="tests/data/data_store",
            filename="csvfile{}.csv".format(str(i)),
        )
        return file

    def check_file(self, a, i):
        assert a.name == "test_file_{}".format(str(i))
        assert a.desc == "Test File {}".format(str(i))
        assert a.folder == "tests/data/data_store"
        assert a.format == "csv"
        assert a.filename == "csvfile{}.csv".format(str(i))
        assert a.compressed is False
        assert a.filepath == os.path.join(a.folder, a.filename)
        assert a.size != 0
        assert isinstance(a.created, datetime)
        assert isinstance(a.modified, datetime)
        assert isinstance(a.accessed, datetime)

    def check_files(self, a, b):
        assert a.name == b.name
        assert a.desc == b.desc
        assert a.folder == b.folder
        assert a.format == b.format
        assert a.filename == b.filename
        assert a.compressed == b.compressed
        assert a.filepath == b.filepath
        assert a.size == b.size
        assert a.created == b.created
        assert a.modified == b.modified
        assert a.accessed == b.accessed

    def test_add(self, caplog, filecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filecontext.begin_transaction()
        dao = DAO(filecontext)

        for i in range(1, 5):
            file1 = self.create_file(i)
            assert file1.id == 0
            file2 = dao.add(file1)
            assert file2.id != 0
            file3 = dao.find(i)
            self.check_files(file1, file2)
            self.check_files(file2, file3)

        filecontext.commit()

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_find(self, caplog, filecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DAO(filecontext)
        for i in range(1, 5):
            file = dao.find(i)
            self.check_file(file, i)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_findall(self, caplog, filecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DAO(filecontext)
        files = dao.findall()
        for file in files:
            id = file.id
            self.check_file(file, id)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    # File is immutable except for internal updates to size and create/modify/access variables.
    # def test_update(self, caplog, filecontext):
    #     logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    #     dao = DAO(filecontext)
    #     files = dao.findall()
    #     for file in files:
    #         file2 = deepcopy(file)
    #         file2.name = "updated_name: {}".format(str(file2.id))
    #         dao.update(file2)
    #         file3 = dao.find(file2.id)
    #         assert file2.name == file3.name
    #         assert not file.name == file3.name

    #     logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_rollback(self, caplog, filecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DAO(filecontext)

        for i in range(5, 9):
            file = self.create_file(i)
            file = dao.add(file)
            self.check_file(file, i)

        files = dao.findall()
        assert len(files) == 8

        filecontext.rollback()
        files = dao.findall()
        assert len(files) == 4

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
