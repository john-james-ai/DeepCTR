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
# Created    : Friday May 13th 2022 07:05:02 pm                                                    #
# Modified   : Friday May 13th 2022 07:05:02 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import shutil
import pytest
import logging
import logging.config
from pyspark.sql import DataFrame

from deepctr.dal.base import FilePathFinder
from deepctr.dal.file import FileAccessObject
from deepctr.dal.params import EntityParams
from deepctr.utils.printing import Printer
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


HOME = "tests/data/dal/test_file/"
DATASET = "vesuvio"
DATASOURCE = "alibaba"
STAGE = "staged"


def get_params(format: str):
    params = EntityParams(
        datasource=DATASOURCE,
        dataset=DATASET,
        stage=STAGE,
        home=HOME,
        name="avoidance",
        format=format,
    )
    return params


@pytest.mark.dal_file
class TestFileAccessObject:

    FORMAT = "csv"

    def test_create(self, caplog, spark_dataframe) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(format=TestFileAccessObject.FORMAT)
        fao = FileAccessObject()
        fao.create(params=params, data=spark_dataframe, force=False)

        path = FilePathFinder().get_path(params)
        assert os.path.exists(path), "File not created"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_create_already_exists(self, caplog, spark_dataframe) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(format=TestFileAccessObject.FORMAT)
        fao = FileAccessObject()
        with pytest.raises(FileExistsError):
            fao.create(params=params, data=spark_dataframe, force=False)

        path = FilePathFinder().get_path(params)
        assert os.path.exists(path), "File not created"

    def test_create_already_exists_overwrite(self, caplog, spark_dataframe) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(format=TestFileAccessObject.FORMAT)
        fao = FileAccessObject()
        fao.create(params=params, data=spark_dataframe, force=True)

        path = FilePathFinder().get_path(params)
        assert os.path.exists(path), "File not created"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(format=TestFileAccessObject.FORMAT)
        fao = FileAccessObject()
        sdf = fao.read(params=params)

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(sdf, title)

        assert isinstance(sdf, DataFrame), logging.error(
            "SparkCSV failed to return a pyspark.sql.DataFrame object"
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read_fail(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(format=TestFileAccessObject.FORMAT)
        params.name = "adlnaos"
        fao = FileAccessObject()
        with pytest.raises(FileNotFoundError):
            fao.read(params=params)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_teardown(self):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree(HOME, ignore_errors=True)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def show(self, sdf, title):
        printer = Printer()
        printer.print_spark_dataframe_summary(content=sdf, title=title)
