#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /io.py                                                                                #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, April 15th 2022, 11:00:20 pm                                                  #
# Modified : Monday, April 25th 2022, 4:26:53 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from abc import ABC, abstractmethod
import pandas as pd
import shutil
from typing import Any
import logging

from deepctr.utils.io import SparkCSV, Parquet
from deepctr.operators.base import Operator
from deepctr.utils.decorators import operator
from deepctr.utils.io import FileManager

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                           IO                                                     #
# ------------------------------------------------------------------------------------------------ #


class IO(Operator, ABC):
    """Abstract base class for IO operators."""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(IO, self).__init__(task_no=task_no, task_name=task_name, task_description=task_description, params=params)

        self._item = params.get("item", None)
        self._stage = params.get("stage", "raw")
        self._fileformat = params.get("fileformat", None)

    @abstractmethod
    def execute(self, data: Any = None, context: dict = None) -> pd.DataFrame:
        pass

    def _check_in(self, context: dict) -> str:
        """Gets an available filepath for writing."""
        asset_type = context.get("asset_type", "data")
        collection = context.get("dataset")
        mode = context.get("mode", "dev")

        fm = FileManager()

        return fm.check_in(
            asset_type=asset_type,
            collection=collection,
            item=self._item,
            stage=self._stage,
            fileformat=self._fileformat,
            mode=mode,
        )

    def _check_out(self, context: dict) -> str:
        """Returns a filepath for an existing asset."""
        asset_type = context.get("asset_type", "data")
        collection = context.get("dataset")
        mode = context.get("mode", "dev")

        fm = FileManager()

        return fm.check_out(
            asset_type=asset_type,
            collection=collection,
            item=self._item,
            stage=self._stage,
            fileformat=self._fileformat,
            mode=mode,
        )


# ------------------------------------------------------------------------------------------------ #
#                                       PARQUET                                                    #
# ------------------------------------------------------------------------------------------------ #


class ParquetReader(IO):
    """Parquet file reader operator for DAGS"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(ParquetReader, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: dict = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        filepath = self._check_out(context)

        io = Parquet()
        data = io.read(filepath=filepath,)

        return data


# ------------------------------------------------------------------------------------------------ #


class ParquetWriter(IO):
    """Writes DataFrames to Parquet file storage"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(ParquetWriter, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: dict = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        filepath = self._check_in(context)
        partition_by = self._params.get("partition_by", None)

        print("\n\n\tDataFrame has shape ({},{})".format(str(data.count()), str(len(data.columns))))

        io = Parquet()
        io.write(data=data, filepath=filepath, partition_by=partition_by)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                          CSV                                                     #
# ------------------------------------------------------------------------------------------------ #


class SparkCSVReader(IO):
    """CSV Reader using Spark API"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(SparkCSVReader, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: dict = None) -> Any:
        """Reads from the designated resource"""
        filepath = self._check_out(context)

        io = SparkCSV()
        data = io.read(filepath=filepath)
        return data


# ------------------------------------------------------------------------------------------------ #


class SparkCSVWriter(IO):
    """CSV Writer using Spark API"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(SparkCSVWriter, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: dict = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        filepath = self._check_in(context)

        io = SparkCSV()
        io.write(data=data, filepath=filepath, partition_by=self._params("partition_by", None))
        return data


# ------------------------------------------------------------------------------------------------ #


class CopyOperator(Operator):
    """Write operator for DAGS"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(CopyOperator, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: dict = None) -> pd.DataFrame:
        """Copies a file from source to destination"""

        source = self._params["source"]
        destination = self._params["destination"]
        shutil.copy(source, destination)

        return None
