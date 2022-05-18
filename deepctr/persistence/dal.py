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

from deepctr.persistence.io import SparkCSV, SparkParquet, S3, TarGZ
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


# ------------------------------------------------------------------------------------------------ #
#                                DATA PARAMETER OBJECT                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataParam:

    datasource: str  # data source, i.e. 'alibaba', 'criteo', 'avazu'
    dataset: str  # The specific data collection
    stage: str  # Data processing stage, i.e 'raw', 'staged', 'interim', 'clean', 'processed'
    data: DataFrame = None  # Spark DataFrame
    bucket: str = None  # The name of the S3 bucket
    object: str = None  # The object key for an S3 resource
    folder: str = None  # The folder within the S3 bucket containing the data.
    filename: str = None  # The name of the file within a dataset.
    source: str = None  # Used for operations requiring a source and destination
    destination: str = None  # Used for operations requiring a source and destination
    home: str = "data"
    format: str = "parquet"  # Storage format, either 'csv', or 'parquet'
    force: bool = False  # Controls whether to override existing data


# ------------------------------------------------------------------------------------------------ #
#                                DATA ACCESS OBJECTS                                               #
# ------------------------------------------------------------------------------------------------ #


class DAO(ABC):
    """Defines interface for data access objects."""

    @abstractmethod
    def create(self, dparam: Any, data: Any, force: bool = False) -> None:
        pass

    @abstractmethod
    def read(self, dparam: Any) -> None:
        pass

    @abstractmethod
    def delete(self, dparam: Any) -> None:
        pass

    @abstractmethod
    def download(self, dparam: DataParam) -> None:
        pass

    @abstractmethod
    def _get_filepath(self, dparam: Any) -> str:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                     DATA REPOSITORY                                              #
# ------------------------------------------------------------------------------------------------ #


class DataAccessObject(DAO):
    """Data access object for data tables."""

    __stages = ["raw", "staged", "interim", "clean", "processed", "extract"]
    __datasource = ["alibaba", "avazu", "criteo"]
    __formats = ["csv", "parquet"]

    # -------------------------------------------------------------------------------------------- #
    def create(self, dparam: DataParam, data: DataFrame, force: str = True) -> None:
        """Persists a new data table to storage.

        Args:
            dparam (DataParam): Data transfer object containing the datatable parameters
            data (DataFrame): The data to store
            force (bool): If True, method will overwrite existing data. Default is True

        """
        filepath = self._get_filepath(dparam)
        if os.path.exists(filepath) and not force:
            raise FileExistsError("{} already exists.".format(filepath))

        io = self._get_io(format=dparam.format)
        io.write(data=data, filepath=filepath)

    # -------------------------------------------------------------------------------------------- #
    def read(self, dparam: DataParam) -> DataFrame:
        """Obtains a DataFrame from persisted storage

        Args:
            dparam (DataParam): Data transfer object containing the datatable parameters

        Returns (DataFrame)
        """
        filepath = self._get_filepath(dparam)

        try:
            io = self._get_io(format=dparam.format)
            return io.read(filepath=filepath)
        except FileNotFoundError as e:
            logger.error("File {} not found.".format(filepath))
            raise FileNotFoundError(e)

    # -------------------------------------------------------------------------------------------- #
    def delete(self, dparam: DataParam) -> None:
        """Removes a data table from persisted storage

        Args:
            dparam (DataParam): Data transfer object containing the datatable parameters
        """
        filepath = self._get_filepath(dparam)
        shutil.rmtree(filepath, ignore_errors=True)

    # -------------------------------------------------------------------------------------------- #
    def download(self, dparam: DataParam) -> None:
        """Downloads data from an S3 Resource

        Args:
            dparam (DataParam): Data transfer object containing the datatable parameters
        """
        directory = self._get_directory(dparam)
        folder = os.path.join(dparam.datasource, dparam.dataset)

        io = S3()
        io.download_directory(
            bucket=dparam.bucket, folder=folder, directory=directory, force=dparam.force,
        )

    # -------------------------------------------------------------------------------------------- #
    def extract(self, dparam: DataParam) -> None:
        """Extracts and decompresses data from Tar GZip  Archives
        Args:
            dparam (DataParam): Data transfer object containing the datatable parameters
        """
        directory = os.path.join(dparam.home, dparam.datasource, dparam.dataset, dparam.source)
        destination = os.path.join(
            dparam.home, dparam.datasource, dparam.dataset, dparam.destination
        )
        io = TarGZ()
        files = os.listdir(directory)
        for file in files:
            source = os.path.join(directory, file)
            io.expand(source=source, destination=destination, force=dparam.force)

    # -------------------------------------------------------------------------------------------- #
    def _get_filepath(self, dparam: DataParam) -> str:
        try:
            datasource = get_close_matches(dparam.datasource, DataAccessObject.__datasource)[0]
            stage = get_close_matches(dparam.stage, DataAccessObject.__stages)[0]
            format = get_close_matches(dparam.format, DataAccessObject.__formats)[0]
            return (
                os.path.join(dparam.home, datasource, dparam.dataset, stage, dparam.filename)
                + "."
                + format
            )

        except IndexError as e:
            raise ValueError("Unable to parse dataset configuration. {}".format(e))

    # -------------------------------------------------------------------------------------------- #
    def _get_directory(self, dparam: DataParam) -> str:
        try:
            datasource = get_close_matches(dparam.datasource, DataAccessObject.__datasource)[0]
            stage = get_close_matches(dparam.stage, DataAccessObject.__stages)[0]

        except IndexError as e:
            raise ValueError("Unable to parse dataset configuration. {}".format(e))
        return os.path.join(dparam.home, datasource, dparam.dataset, stage)

    # -------------------------------------------------------------------------------------------- #
    def _get_io(self, format: str) -> Union[SparkCSV, SparkParquet]:
        if "csv" in format:
            io = SparkCSV()
        else:
            io = SparkParquet()
        return io
