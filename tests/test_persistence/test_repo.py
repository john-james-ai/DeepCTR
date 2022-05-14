#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_repo.py                                                                       #
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
import logging
from deepctr.persistence.repository import DataRepository
from deepctr.utils.printing import Printer

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.repo
class TestDataRepo:
    def test_add(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        asset = parquet_asset["asset"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        mode = parquet_asset["mode"]
        filepath = parquet_asset["filepath"]
        force = False

        # Add asset to repo
        repo = DataRepository()
        repo.add(
            name=name,
            asset=asset,
            dataset=dataset,
            stage=stage,
            format=format,
            mode=mode,
            force=force,
        )

        # Check existence of file and location
        assert os.path.exists(filepath), "TestDataRepo: add failed"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_add_ok(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        asset = parquet_asset["asset"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        mode = parquet_asset["mode"]
        filepath = parquet_asset["filepath"]
        force = True

        # Add asset to repo
        repo = DataRepository()
        repo.add(
            name=name,
            asset=asset,
            dataset=dataset,
            stage=stage,
            format=format,
            mode=mode,
            force=force,
        )

        # Check existence of file and location
        assert os.path.exists(filepath), "TestDataRepo: add failed"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_add_fail(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        asset = parquet_asset["asset"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        mode = parquet_asset["mode"]
        force = False

        repo = DataRepository()
        with pytest.raises(FileExistsError):
            repo.add(
                name=name,
                asset=asset,
                dataset=dataset,
                stage=stage,
                format=format,
                mode=mode,
                force=force,
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_get(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        asset = parquet_asset["asset"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        mode = parquet_asset["mode"]

        # Read asset from repository
        repo = DataRepository()
        sdf = repo.get(name=name, dataset=dataset, stage=stage, format=format, mode=mode)

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(sdf, title)

        assert sdf.count() == asset.count(), logger.error("TestDataRepo: add failed")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_get_file_not_found(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = "test_asset_not_to_be_found"
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        mode = parquet_asset["mode"]

        # Read asset from repository
        repo = DataRepository()

        with pytest.raises(FileNotFoundError):
            repo.get(name=name, dataset=dataset, stage=stage, format=format, mode=mode)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_get_parse_error(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = "test_asset_not_to_be_found"
        dataset = parquet_asset["dataset"]
        stage = "adfala;"
        format = parquet_asset["format"]
        mode = parquet_asset["mode"]

        # Read asset from repository
        repo = DataRepository()

        with pytest.raises(ValueError):
            repo.get(name=name, dataset=dataset, stage=stage, format=format, mode=mode)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_remove(self, caplog, parquet_asset) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain asset
        name = parquet_asset["name"]
        dataset = parquet_asset["dataset"]
        stage = parquet_asset["stage"]
        format = parquet_asset["format"]
        mode = parquet_asset["mode"]
        filepath = parquet_asset["filepath"]

        assert os.path.exists(filepath), logger.error("TestDataRepo: remove - already removed.")
        # Add asset to repo
        repo = DataRepository()
        repo.remove(name=name, dataset=dataset, stage=stage, format=format, mode=mode)

        assert not os.path.exists(filepath), logger.error("TestDataRepo: remove failed")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def show(self, sdf, title):
        printer = Printer()
        printer.print_spark_dataframe_summary(content=sdf, title=title)
