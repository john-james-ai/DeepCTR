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
from datetime import datetime
import pandas as pd
import logging.config
import pyspark
from pyspark.sql import SparkSession, DataFrame
import findspark
import pyarrow.parquet as pq
from typing import Union

from deepctr.data.base import Metadata
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
    def read(self, filepath: str, **kwargs) -> Union[pd.DataFrame, DataFrame]:
        pass

    @abstractmethod
    def write(self, data: Union[pd.DataFrame, DataFrame], filepath: str, **kwargs) -> None:
        pass

    @abstractmethod
    def metadata(self, filepath: str, **kwargs) -> dict:
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

    def metadata(self, filepath: str) -> dict:
        """Returns select metadata for a parquet file.

        Args:
            filepath (str): Path to parquet file

        Returns:
            dictionary select metadata
        """
        if os.path.exists(filepath):
            pf = pq.ParquetFile(filepath)
            result = os.stat(filepath)
            metadata = Metadata(
                size=result.st_size,
                rows=pf.metadata.num_rows,
                cols=pf.metadata.num_columns,
                created=datetime.fromtimestamp(result.st_ctime),
                modified=datetime.fromtimestamp(result.st_mtime),
                accessed=datetime.fromtimestamp(result.st_atime),
            )
        else:
            # A default metadata object is returned if the file doesn't exist
            # The File object will be updated when the small file containing dataw
            # is saved to disk.
            metadata = Metadata()
        return metadata


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

    def metadata(self, filepath: str) -> dict:
        """Returns select metadata for a spark csv file.

        Args:
            filepath (str): Path to csv file

        Returns:
            dictionary select metadata
        """
        if os.path.exists(filepath):
            result = os.stat(filepath)
            df = self.read(filepath)
            metadata = Metadata(
                size=result.st_size,
                rows=df.count(),
                cols=len(df.columns),
                created=datetime.fromtimestamp(result.st_ctime),
                modified=datetime.fromtimestamp(result.st_mtime),
                accessed=datetime.fromtimestamp(result.st_atime),
            )
        else:
            # A default metadata object is returned if the file doesn't exist
            # The File object will be updated when the small file containing dataw
            # is saved to disk.
            metadata = Metadata()

        return metadata
