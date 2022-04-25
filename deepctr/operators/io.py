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
import os
import pandas as pd
import shutil
from typing import Any

from deepctr.utils.io import CsvIO
from deepctr.operators.base import Operator
from deepctr.utils.decorators import operator
from deepctr.data.dag import Context


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
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        pass

    def _get_filepath(self, context: dict) -> str:
        """Formats the filepath for the context mode and source file"""
        mode = context.get("mode")
        directory = context.get(mode)
        filename = self._params["filename"]
        return os.path.join(directory, filename)


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
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        filepath = self._get_filepath(context)

        io = CsvIO()
        if self._params.get("usecols", None):
            data = io.read(
                filepath=self._params["source"],
                header=0,
                usecols=self._params["usecols"],
            )
        else:
            data = io.read(filepath=filepath, header=0)

        return data


# ------------------------------------------------------------------------------------------------ #


class CSVWriter(IO):
    """Write operator for DAGS"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(CSVWriter, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        filepath = self._get_filepath(context)

        io = CsvIO()
        io.write(data=data, filepath=filepath)
        return None


# ------------------------------------------------------------------------------------------------ #


class SQLReader(IO):
    """Reads an SQL file and returns a list of sql statements."""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(SQLReader, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        io = CsvIO()
        io.write(data=data, filepath=self._params["destination"])
        return None


# ------------------------------------------------------------------------------------------------ #


class CopyOperator(Operator):
    """Write operator for DAGS"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(CopyOperator, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        """Copies a file from source to destination"""

        source = self._params["source"]
        destination = self._params["destination"]
        shutil.copy(source, destination)

        return None
