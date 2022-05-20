#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deepctr: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /io.py                                                                                #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 6:41:17 pm                                              #
# Modified : Tuesday, May 3rd 2022, 8:48:33 pm                                                     #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
from abc import ABC, abstractmethod
import os
import logging
import pickle
import logging.config
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import findspark
from typing import Any

from deepctr.utils.log_config import LOG_CONFIG

findspark.init()

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
#                                              IO                                                  #
# ------------------------------------------------------------------------------------------------ #


class IO(ABC):
    """Base class for IO classes"""

    @abstractmethod
    def read(self, filepath: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: Any, filepath: str, **kwargs) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                         SPARK PARQUET                                            #
# ------------------------------------------------------------------------------------------------ #


class SparkParquet(IO):
    """Reads, and writes Spark DataFrames to / from Parquet storage format.."""

    def read(self, filepath: str, cores: int = 18) -> pyspark.sql.DataFrame:
        """Reads a Spark DataFrame from Parquet file resource

        Args:
            filepath (str): The path to the parquet file resource

        Returns:
            Spark DataFrame
        """

        if os.path.exists(filepath):
            local = "local[" + str(cores) + "]"
            spark = SparkSession.builder.master(local).appName("Read SparkParquet").getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            return spark.read.parquet(filepath)

        else:
            logger.error("File {} was not found.".format(filepath))
            raise FileNotFoundError()

    def write(
        self,
        data: pyspark.sql.DataFrame,
        filepath: str,
        header: bool = True,
        mode: str = "overwrite",
        **kwargs,
    ) -> None:
        """Writes Spark DataFrame to Parquet file resource

        Args:
            data (pyspark.sql.DataFrame): Spark DataFrame to write
            filepath (str): The path to the parquet file to be written
            header (bool): True if data contains header row. False otherwise.
            mode (str): 'overwrite' or 'append'. Default is 'overwrite'.
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        data.write.option("header", header).mode(mode).parquet(filepath)


# ------------------------------------------------------------------------------------------------ #
#                                          SPARK CSV                                               #
# ------------------------------------------------------------------------------------------------ #
class SparkCSV(IO):
    """IO using the Spark API"""

    def read(
        self,
        filepath: str,
        cores: int = 18,
        header: bool = True,
        infer_schema: bool = True,
        sep: str = ",",
    ) -> pyspark.sql.DataFrame:
        """Reads a Spark DataFrame from Parquet file resource

        Args:
            filepath (str): The path to the parquet file resource
            cores (int): The number of CPU cores to designated to the operation. Default = 18
            header (bool): True if the data contains a header row. Default = True
            infer_schema (bool): True if the data types should be inferred. Default = False
            sep (str): Column delimiter. Default = ","

        Returns:
            Spark DataFrame
        """

        if os.path.exists(filepath):
            local = "local[" + str(cores) + "]"
            spark = SparkSession.builder.master(local).appName("Read SparkCSV").getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            return spark.read.options(header=header, delimiter=sep, inferSchema=infer_schema).csv(
                filepath
            )
        else:
            logger.error("File {} was not found.".format(filepath))
            raise FileNotFoundError()

    def write(
        self,
        data: pyspark.sql.DataFrame,
        filepath: str,
        header: bool = True,
        sep: str = ",",
        mode: str = "overwrite",
    ) -> None:
        """Writes Spark DataFrame to Parquet file resource

        Args:
            data (pyspark.sql.DataFrame): Spark DataFrame to write
            filepath (str): The path to the parquet file to be written
            header (bool): True if data contains header. Default = True
            sep (str): Column delimiter. Default = ","
            mode (str): 'overwrite' or 'append'. Default = 'overwrite'.
        """

        data.write.csv(path=filepath, header=header, sep=sep, mode=mode)


# ------------------------------------------------------------------------------------------------ #
#                                       PICKLER                                                    #
# ------------------------------------------------------------------------------------------------ #
class Pickler(IO):
    """Wrapper for Pickle class"""

    def read(self, filepath: str) -> Any:
        """Loads serialized data

        Args:
            filepath (str): Path to the serialized resource
        """

        with open(filepath, "rb") as f:
            return pickle.load(f)

    def write(self, data: Any, filepath: str) -> None:
        """Serializes the data using the python pickle module.

        Args:
            data (Any): The data to be serialized
            filepath (str): Path to the serialized resource
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "wb") as f:
            pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
