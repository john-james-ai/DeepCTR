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
# Modified   : Tuesday June 28th 2022 12:50:43 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import logging
import logging.config
from datetime import datetime
from copy import deepcopy

from deepctr.dal.source import Source
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
class TestFile:
    def test_create(self, caplog, parquet_file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        assert parquet_file.name == "test_parquet_file"
        assert parquet_file.desc == "Test Parquet File"
        assert parquet_file.folder == "tests/data/data_store"
        assert parquet_file.format == "parquet"
        assert parquet_file.filename == "file.parquet"
        assert parquet_file.compressed is False
        assert parquet_file.filepath == os.path.join(parquet_file.folder, parquet_file.filename)
        assert parquet_file.size != 0
        assert isinstance(parquet_file.created, datetime)
        assert isinstance(parquet_file.modified, datetime)
        assert isinstance(parquet_file.accessed, datetime)
        assert isinstance(parquet_file.file_created, datetime)
        assert isinstance(parquet_file.file_modified, datetime)
        assert isinstance(parquet_file.file_accessed, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_add(self, caplog, sourcecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        sourcecontext.begin_transaction()
        dao = DAO(sourcecontext)

        for i in range(1, 5):
            source = Source(name=self.name(i), desc=self.desc(i), url=self.url(i))
            source2 = dao.add(source)
            self.check_result(source2, i)

        sourcecontext.commit()

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_find(self, caplog, source, sourcecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DAO(sourcecontext)
        for i in range(1, 5):
            source2 = dao.find(i)
            self.check_result(source2, i)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_findall(self, caplog, sourcecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DAO(sourcecontext)
        sources = dao.findall()
        for source in sources:
            id = source.id
            self.check_result(source, id)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_update(self, caplog, sourcecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DAO(sourcecontext)
        sources = dao.findall()
        for source in sources:
            source2 = deepcopy(source)
            source2.name = "updated_name: {}".format(str(source2.id))
            dao.update(source2)
            source3 = dao.find(source2.id)
            assert source2.name == source3.name
            assert not source.name == source3.name

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_rollback(self, caplog, sourcecontext):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        sourcecontext.rollback()
        dao = DAO(sourcecontext)
        for i in range(1, 5):
            source = dao.find(i)
            self.check_result(source, i)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

