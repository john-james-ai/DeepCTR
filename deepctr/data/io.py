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
# URL      : https://github.com/john-james-ai/deepctr                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, March 22nd 2022, 4:02:42 am                                                  #
# Modified : Saturday, April 9th 2022, 3:02:48 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
from abc import ABC, abstractmethod
import pandas as pd
from typing import Any

from deepctr.utils.io import CsvIO
from deepctr.data.base import Operator
from deepctr.utils.decorators import operator

# ------------------------------------------------------------------------------------------------ #
#                                           IO                                                     #
# ------------------------------------------------------------------------------------------------ #


class IO(Operator, ABC):
    """Abstract base class for IO operators."""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(IO, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @abstractmethod
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                          CSV                                                     #
# ------------------------------------------------------------------------------------------------ #


class CSVReader(IO):
    """Read operator for DAGS"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(CSVReader, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        io = CsvIO()
        if self._params.get("usecols", None):
            data = io.load(
                filepath=self._params["filepath"],
                header=0,
                usecols=self._params["usecols"],
            )
        else:
            data = io.load(filepath=self._params["filepath"], header=0)

        return data


# ------------------------------------------------------------------------------------------------ #


class CSVWriter(IO):
    """Write operator for DAGS"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(CSVWriter, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        io = CsvIO()
        io.save(data=data, filepath=self._params["filepath"])
        return None


# ------------------------------------------------------------------------------------------------ #
#                                      AMAZON S3                                                   #
# ------------------------------------------------------------------------------------------------ #
class S3Reader(IO):
    """Read data from an Amazon S3 file into a dataframe"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(S3Reader, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

        filepath = self._params["filepath"]

        df = pd.read_csv(
            f"s3://{AWS_S3_BUCKET}/{filepath}",
            storage_options={"key": AWS_ACCESS_KEY_ID, "secret": AWS_SECRET_ACCESS_KEY},
        )
        return df


# ------------------------------------------------------------------------------------------------ #
class S3Writer(IO):
    """Writes a DataFrame to an Amazon S3 bucket"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(S3Writer, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

        filepath = self._params["filepath"]

        data.to_csv(
            f"s3://{AWS_S3_BUCKET}/{filepath}",
            index=False,
            storage_options={"key": AWS_ACCESS_KEY_ID, "secret": AWS_SECRET_ACCESS_KEY},
        )
