#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_dao.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 26th 2022 07:03:22 pm                                                  #
# Modified   : Tuesday June 28th 2022 09:19:59 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
# Remember to propagate any changes made to the database to test_dao_setup.sql
import inspect
import pytest
import logging
import logging.config

from deepctr.dal.file import File
from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.dao import DAO

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                   TEST DATASET DAO                                               #
# ================================================================================================ #


@pytest.mark.dao
@pytest.mark.datasetdao
class TestDAO:
    def print_dates(self, a, b):
        print("\n", 50 * "=")
        print("{} created: {}\t{} created: {}".format(a.name, a.created, b.name, b.created))
        print("{} modified: {}\t{} modified: {}".format(a.name, a.modified, b.name, b.modified))
        print("{} accessed: {}\t{} accessed: {}".format(a.name, a.accessed, b.name, b.accessed))

    def datasets_equal(self, a, b) -> bool:
        assert a.name == b.name
        assert a.source == b.source
        assert a.file_system == b.file_system
        assert a.stage_id == b.stage_id
        assert a.stage_name == b.stage_name
        assert a.home == b.home
        assert a.bucket == b.bucket
        assert a.folder == b.folder
        assert a.format == b.format
        assert a.size == b.size
        return True

    def files_equal(self, a, b) -> bool:
        assert a.name == b.name
        assert a.source == b.source
        assert a.dataset_id == b.dataset_id
        assert a.dataset == b.dataset
        assert a.file_system == b.file_system
        assert a.stage_id == b.stage_id
        assert a.stage_name == b.stage_name
        assert a.home == b.home
        assert a.bucket == b.bucket
        assert a.filepath == b.filepath
        assert a.compressed == b.compressed
        assert a.size == b.size
        assert a.rows == b.rows
        assert a.cols == b.cols
        assert a.exists == b.exists
        assert a.format == b.format
        assert a.size == b.size
        assert a.created == b.created
        assert a.modified == b.modified
        assert a.accessed == b.accessed
        return True

    def dataset_file(self, d, f) -> bool:

        assert d.source == f.source
        assert d.file_system == f.file_system
        assert d.stage_id == f.stage_id
        assert d.stage_name == f.stage_name
        assert d.home == f.home
        assert d.bucket == f.bucket
        assert d.format == f.format
        return True

    def test_insert(self, caplog, dataset, filedatasetcontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filecontext = filedatasetcontext["file"]
        datasetcontext = filedatasetcontext["dataset"]

        filecontext.begin_transaction()

        # Insert Dataset
        dao = DAO(datasetcontext)
        assert dataset.id == 0
        dataset2 = dao.add(dataset)
        assert dataset2.id == 1
        assert self.datasets_equal(dataset, dataset2)

        # Insert Files
        dao = DAO(filecontext)
        files = dataset.files.values()
        for i, file in enumerate(files):
            assert file.id == 0
            file2 = dao.add(file)
            assert file.id == i + 1
            self.print_dates(file, file2)
            assert self.files_equal(file, file2)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_find(self, caplog, dataset, filedatasetcontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filecontext = filedatasetcontext["file"]
        datasetcontext = filedatasetcontext["dataset"]

        # Dataset
        dao = DAO(datasetcontext)
        dataset2 = dao.find(1)
        assert dataset.id == dataset2.id
        assert self.datasets_equal(dataset, dataset2)

        # File
        dao = DAO(filecontext)
        files = dataset.files.values()
        for i in range(1, len(files) + 1):
            file = dao.find(i)
            assert file.id == i
            assert self.dataset_file(dataset, file)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_findall(self, caplog, dataset, filedatasetcontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filecontext = filedatasetcontext["file"]
        datasetcontext = filedatasetcontext["dataset"]

        # Dataset
        dao = DAO(datasetcontext)
        dataset2 = dao.findall()[0]
        assert self.datasets_equal(dataset, dataset2)

        filecontext.commit()

        # Files
        dao = DAO(filecontext)
        files = dao.findall()
        for file in files:
            assert isinstance(file, File)
            assert file.id in range(1, len(files) + 1)
            assert self.dataset_file(dataset, file)
        # dao.commit()  # I don't think we need this commit. All contexts share same connection.
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_update(self, caplog, dataset, filedatasetcontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filecontext = filedatasetcontext["file"]
        datasetcontext = filedatasetcontext["dataset"]

        # Dataset
        dataset2 = dataset
        dao = DAO(datasetcontext)
        dataset2.compressed = True
        dataset2.file_system = "s3"
        dao.update(dataset2)
        dataset3 = dao.find(dataset2.id)
        assert self.datasets_equal(dataset2, dataset3)

        # File
        dao = DAO(filecontext)
        files = dataset.files.values()
        for file in files:
            file.compressed = True
            file.file_system = "s3"
            dao.update(file)
            file2 = dao.find(file.id)
            assert file2.id == file.id
            assert self.files_equal(file, file2)

        filecontext.rollback()

        # Confirm rollback on dataset
        dao = DAO(datasetcontext)
        dataset2 = dao.find(dataset.id)
        assert dataset2.compressed is False
        assert dataset2.file_system == "local"

        # Confirm rollback on files
        dao = DAO(filecontext)
        files = dao.findall()
        for file in files:
            assert file.compressed is False
            assert file.file_system == "local"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_delete(self, caplog, dataset, filedatasetcontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filecontext = filedatasetcontext["file"]
        datasetcontext = filedatasetcontext["dataset"]

        # Delete Dataset
        dao = DAO(datasetcontext)
        dataset = dao.findall()[0]
        dao.delete(dataset.id)
        assert not dao.exists(dataset.id)

        # Delete Files
        dao = DAO(filecontext)
        files = dao.findall()
        for file in files:
            dao.delete(file.id)
            assert not dao.exists(file.id)
