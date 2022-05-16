#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /dal.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 02:51:48 pm                                                    #
# Modified   : Sunday May 15th 2022 11:22:36 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module defines the API for data access and management."""
import os
from abc import ABC, abstractmethod
import logging
import logging.config
from dataclasses import dataclass
from typing import Any, Union
from difflib import get_close_matches
import shutil

from pyspark.sql import DataFrame

from deepctr.persistence.io import SparkCSV, SparkParquet
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


# ------------------------------------------------------------------------------------------------ #
#                                DATA TRANSFER OBJECTS                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataTableDTO:
    name: str  # The name of the table.
    dataset: str  # Name of dataset.
    asset: str  # The data asset, i.e. 'alibaba', 'criteo', 'avazu'
    stage: str  # Data processing stage, i.e 'raw', 'staged', 'interim', 'clean', 'processed'
    env: str  # Either 'dev', 'prod', or 'test' environment
    format: str = "parquet"  # Storage format, either 'csv', or 'parquet'


# ------------------------------------------------------------------------------------------------ #
#                                DATA ACCESS OBJECTS                                               #
# ------------------------------------------------------------------------------------------------ #


class DAO(ABC):
    """Defines interface for data access objects."""

    @abstractmethod
    def create(self, dto: Any, data: Any, force: bool = False) -> None:
        pass

    @abstractmethod
    def read(self, dto: Any) -> None:
        pass

    @abstractmethod
    def delete(self, dto: Any) -> None:
        pass

    @abstractmethod
    def _get_filepath(self, dto: Any) -> str:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                     DATA REPOSITORY                                              #
# ------------------------------------------------------------------------------------------------ #


class DataTableDAO(DAO):
    """Data access object for data tables."""

    __stages = ["raw", "staged", "interim", "clean", "processed"]
    __envs = ["dev", "prod", "test"]
    __assets = ["alibaba", "avazu", "criteo"]
    __formats = ["csv", "parquet"]
    __asset_type = "data"

    # -------------------------------------------------------------------------------------------- #
    def create(self, dto: DataTableDTO, data: DataFrame, force: str = True) -> None:
        """Persists a new data table to storage.

        Args:
            dto (DataTableDTO): Data transfer object containing the datatable parameters
            data (DataFrame): The data to store
            force (bool): If True, method will overwrite existing data. Default is True

        """
        filepath = self._get_filepath(dto)
        if os.path.exists(filepath) and not force:
            raise FileExistsError("{} already exists.".format(filepath))

        io = self._get_io(format=dto.format)
        io.write(data=data, filepath=filepath)

    # -------------------------------------------------------------------------------------------- #
    def read(self, dto: DataTableDTO) -> DataFrame:
        """Obtains a DataFrame from persisted storage

        Args:
            dto (DataTableDTO): Data transfer object containing the datatable parameters

        Returns (DataFrame)
        """
        filepath = self._get_filepath(dto)

        try:
            io = self._get_io(format=dto.format)
            return io.read(filepath=filepath)
        except FileNotFoundError as e:
            logger.error("File {} not found.".format(filepath))
            raise FileNotFoundError(e)

    # -------------------------------------------------------------------------------------------- #
    def delete(self, dto: DataTableDTO) -> None:
        """Removes a data table from persisted storage

        Args:
            dto (DataTableDTO): Data transfer object containing the datatable parameters
        """
        filepath = self._get_filepath(dto)
        shutil.rmtree(filepath, ignore_errors=True)

    # -------------------------------------------------------------------------------------------- #
    def _get_filepath(self, dto: DataTableDTO) -> str:
        try:
            asset = get_close_matches(dto.asset, DataTableDAO.__assets)[0]
            stage = get_close_matches(dto.stage, DataTableDAO.__stages)[0]
            format = get_close_matches(dto.format, DataTableDAO.__formats)[0]
            env = get_close_matches(dto.env, DataTableDAO.__envs)[0]
        except IndexError as e:
            raise ValueError("Unable to parse asset configuration. {}".format(e))
        return (
            os.path.join(DataTableDAO.__asset_type, env, asset, stage, dto.dataset, dto.name)
            + "."
            + format
        )

    def _get_io(self, format: str) -> Union[SparkCSV, SparkParquet]:
        if "csv" in format:
            io = SparkCSV()
        else:
            io = SparkParquet()
        return io
