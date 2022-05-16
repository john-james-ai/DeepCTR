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
from deepctr.persistence.dal import DataTableDAO, DataTableDTO
from deepctr.utils.printing import Printer

# ------------------------------------------------------------------------------------------------ #
logging.getLogger("py4j").setLevel(logging.WARN)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dao
class TestDataTableDAO:
    def test_setup(self):
        shutil.rmtree("data/test", ignore_errors=True)

    def test_add(self, caplog, parquet_asset) -> None:
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
        force = False

        # Create DTO
        dto = DataTableDTO(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Persist data table
        dao = DataTableDAO()
        dao.create(dto=dto, data=data, force=force)

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

        # Create DTO
        dto = DataTableDTO(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Persist data table
        dao = DataTableDAO()
        dao.create(dto=dto, data=data, force=force)

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

        # Create DTO
        dto = DataTableDTO(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Persist data table
        dao = DataTableDAO()
        with pytest.raises(FileExistsError):
            dao.create(dto=dto, data=data, force=force)

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

        # Create DTO
        dto = DataTableDTO(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Read data table
        dao = DataTableDAO()
        sdf = dao.read(dto=dto)

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

        # Create DTO
        dto = DataTableDTO(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        # Read data table
        dao = DataTableDAO()
        with pytest.raises(FileNotFoundError):
            dao.read(dto=dto)

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

        # Create DTO
        dto = DataTableDTO(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )
        dao = DataTableDAO()
        with pytest.raises(ValueError):
            dao.read(dto=dto)

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

        # Create DTO
        dto = DataTableDTO(
            name=name, dataset=dataset, asset=asset, stage=stage, env=env, format=format
        )

        dao = DataTableDAO()

        dao.delete(dto)

        assert not os.path.exists(filepath), logger.error("TestDataRepo: remove failed")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def show(self, sdf, title):
        printer = Printer()
        printer.print_spark_dataframe_summary(content=sdf, title=title)
