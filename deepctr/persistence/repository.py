#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /repository.py                                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 02:51:48 pm                                                    #
# Modified   : Friday May 13th 2022 02:51:48 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

"""Module defines the API for data access and management."""
import os
from abc import ABC, abstractmethod
import logging
from typing import Any, Union
from difflib import get_close_matches
import shutil

from pyspark.sql import DataFrame

from deepctr.persistence.dal import SparkCSV, SparkParquet

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.INFO)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
#                                      REGISTRY                                                    #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                     REPOSITORY                                                   #
# ------------------------------------------------------------------------------------------------ #


class Repository(ABC):
    """Abstract base class for repositories."""

    @abstractmethod
    def add(self, name: str, asset: Any, **kwargs) -> None:
        pass

    @abstractmethod
    def get(self, name: str, **kwargs) -> None:
        pass

    @abstractmethod
    def remove(self, name: str, **kwargs) -> None:
        pass

    @abstractmethod
    def _get_filepath(self, name: str, **kwargs) -> str:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                     DATA REPOSITORY                                              #
# ------------------------------------------------------------------------------------------------ #


class DataRepository(Repository):
    """Repository for data assets."""

    __stages = ["raw", "staged", "clean", "processed", "complete"]
    __modes = ["dev", "prod", "test"]
    __datasets = ["alibaba", "avazu", "criteo"]
    __formats = ["csv", "parquet"]
    __asset_type = "data"

    # -------------------------------------------------------------------------------------------- #
    def add(
        self,
        name: str,
        asset: DataFrame,
        dataset: str,
        stage: str,
        format: str = "parquet",
        mode: str = "prod",
        force: str = False,
    ) -> None:
        """Adds a data asset (file) to the data repository

        Args:
            name (str): Name of dataset
            asset (DataFrame): The data to store
            dataset (str): Dataset to which the asset belongs
            stage (str): Data processing stage i.e, 'raw', 'staged', 'clean', 'processed'.
            format (str): Either 'csv', or 'parquet'. Default is 'parquet'
            mode (str): Either 'dev' or 'prod'. Default is 'prod'
            force (bool): If True, method will overwrite existing data. Default is False

        """
        filepath = self._get_filepath(
            name=name, dataset=dataset, stage=stage, format=format, mode=mode
        )
        if os.path.exists(filepath) and not force:
            raise FileExistsError("{} already exists.".format(filepath))

        io = self._get_io(format)
        io.write(data=asset, filepath=filepath)

    # -------------------------------------------------------------------------------------------- #
    def get(
        self, name: str, dataset: str, stage: str, format: str = "parquet", mode: str = "prod"
    ) -> DataFrame:
        """Obtains a DataFrame from the data repository

        Args:
            name (str): Name of dataset
            dataset (str): Dataset to which the asset belongs
            stage (str): Data processing stage i.e, 'raw', 'staged', 'clean', 'processed'.
            format (str): Either 'csv', or 'parquet'. Default is 'parquet'
            mode (str): Either 'dev' or 'prod'. Default is 'prod'
            force (bool): If True, method will overwrite existing data. Default is False

        Returns (DataFrame)
        """
        filepath = self._get_filepath(
            name=name, dataset=dataset, stage=stage, format=format, mode=mode
        )

        try:
            io = self._get_io(format)
            return io.read(filepath=filepath)
        except FileNotFoundError as e:
            logger.error("File {} not found.".format(filepath))
            raise FileNotFoundError(e)

    # -------------------------------------------------------------------------------------------- #
    def remove(
        self, name: str, dataset: str, stage: str, format: str = "parquet", mode: str = "prod"
    ) -> None:
        """Removes a file from the data repository

        Args:
            name (str): Name of dataset
            dataset (str): Dataset to which the asset belongs
            stage (str): Data processing stage i.e, 'raw', 'staged', 'clean', 'processed'.
            format (str): Either 'csv', or 'parquet'. Default is 'parquet'
            mode (str): Either 'dev' or 'prod'. Default is 'prod'
            force (bool): If True, method will overwrite existing data. Default is False

        Returns (DataFrame)
        """
        filepath = self._get_filepath(
            name=name, dataset=dataset, stage=stage, format=format, mode=mode
        )
        shutil.rmtree(filepath, ignore_errors=True)

    # -------------------------------------------------------------------------------------------- #
    def _get_filepath(
        self, name: str, dataset: str, stage: str, format: str = "parquet", mode: str = "prod",
    ) -> str:
        try:
            dataset = get_close_matches(dataset, DataRepository.__datasets)[0]
            stage = get_close_matches(stage, DataRepository.__stages)[0]
            format = get_close_matches(format, DataRepository.__formats)[0]
            mode = get_close_matches(mode, DataRepository.__modes)[0]
        except IndexError as e:
            raise ValueError("Unable to parse dataset configuration. {}".format(e))
        return os.path.join(DataRepository.__asset_type, dataset, mode, stage, name) + "." + format

    def _get_io(self, format: str) -> Union[SparkCSV, SparkParquet]:
        if "csv" in format:
            io = SparkCSV()
        else:
            io = SparkParquet()
        return io
