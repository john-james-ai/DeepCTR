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
# Created    : Friday May 13th 2022 09:52:32 pm                                                    #
# Modified   : Friday May 13th 2022 09:53:46 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import os
import inspect
import pytest
import shutil
import logging
import logging.config
from deepctr.persistence.dal import DataTableDAO, DataParam
from deepctr.utils.printing import Printer
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dao
class TestDataTableDAO:
    def test_setup(self):
        shutil.rmtree("data/test/alibaba/stage", ignore_errors=True)

    def test_add(self, caplog, dp_std) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Persist data table
        dao = DataTableDAO()
        dao.create(data_params=dp_std, data=data, force=force)

        # Check existence of file and location
        assert os.path.exists(filepath), "TestDataDAO: add failed"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_add_ok(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        asset = parquet_asset["asset"]
        data = parquet_asset["data"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        env = parquet_asset["env"]
        filepath = parquet_asset["filepath"]
        force = True

        # Create data_params
        data_params = DataParam(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Persist data table
        dao = DataTableDAO()
        dao.create(data_params=data_params, data=data, force=force)

        # Check existence of file and location
        assert os.path.exists(filepath), "TestDataRepo: add failed"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_add_fail(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        asset = parquet_asset["asset"]
        data = parquet_asset["data"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        env = parquet_asset["env"]
        force = False

        # Create data_params
        data_params = DataParam(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Persist data table
        dao = DataTableDAO()
        with pytest.raises(FileExistsError):
            dao.create(data_params=data_params, data=data, force=force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_get(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        asset = parquet_asset["asset"]
        data = parquet_asset["data"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        env = parquet_asset["env"]

        # Create data_params
        data_params = DataParam(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Read data table
        dao = DataTableDAO()
        sdf = dao.read(data_params=data_params)

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(sdf, title)

        assert sdf.count() == data.count(), logger.error("TestDataRepo: add failed")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_get_file_not_found(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = "test_asset_not_to_be_found"
        asset = parquet_asset["asset"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        env = parquet_asset["env"]

        # Create data_params
        data_params = DataParam(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Read data table
        dao = DataTableDAO()
        with pytest.raises(FileNotFoundError):
            dao.read(data_params=data_params)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_get_parse_error(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = "test_asset_not_to_be_found"
        asset = parquet_asset["asset"]
        dataset = parquet_asset["dataset"]
        stage = "x93i"
        format = ".duto"
        env = parquet_asset["env"]

        # Create data_params
        data_params = DataParam(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )
        dao = DataTableDAO()
        with pytest.raises(ValueError):
            dao.read(data_params=data_params)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_remove(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        asset = parquet_asset["asset"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        env = parquet_asset["env"]
        filepath = parquet_asset["filepath"]

        assert os.path.exists(filepath), logger.error("TestDataRepo: remove - already removed.")

        # Create data_params
        data_params = DataParam(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        dao = DataTableDAO()

        dao.delete(data_params)

        assert not os.path.exists(filepath), logger.error("TestDataRepo: remove failed")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def show(self, sdf, title):
        printer = Printer()
        printer.print_spark_dataframe_summary(content=sdf, title=title)
