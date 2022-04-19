#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepNeuralCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /data.py                                                                              #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepNeuralCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, April 16th 2022, 12:50:46 am                                                #
# Modified : Saturday, April 16th 2022, 10:02:31 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import pandas as pd
from typing import Any
import tarfile
from deepctr.utils.decorators import operator
from deepctr.operators.base import Operator
from deepctr.utils.io import CsvIO

# ------------------------------------------------------------------------------------------------ #


class ReplaceColumnNames(Operator):
    """Replace column names in a DataFrame.

    Args:
        task_no (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        task_description (str): A description for the task
        params (Any): Parameters for the task
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(ReplaceColumnNames, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Replaces the columns in the DataFrame according to the params['columns'] object."""

        data.rename(columns=self._params["columns"], inplace=True)

        return data


# ------------------------------------------------------------------------------------------------ #


class ExtractCreate(Operator):
    """Extracts column data from input and creates a DataFrame

    Args:
    task_no (int): A number, typically used to indicate the sequence of the task within a DAG
    task_name (str): String name
    task_description (str): A description for the task
    params (Any): Parameters for the task
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(ExtractCreate, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Extracts data from input and returns a DataFrame."""

        df = data[self._params["columns"]]
        df.index.name = self._params["index"]
        df.drop_duplicates(keep="first", inplace=True, ignore_index=False)

        return df


# ------------------------------------------------------------------------------------------------ #


class MergeReplaceFK(Operator):
    """Class merges and replaces attributes with a foreign key referencing the corresponding entity.

    Designed to address the specific data processing task of replacing the attributes of an
    entity with a foreign key reference to the entity.


    Args:
        task_no (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        task_description (str): A description for the task
        params (Any): Parameters for the task including:
            left (str): The filepath to the left input DataFrame for the merge operation
            right (str): The filepath to the right input DataFrame for the merge operation
            destination(str): The filepath for the output transformed file.
            on (list): A list of attributes to match on the left and right DataFrames
            replace_left (list): List of attributes to replace on the left dataframe
            with_right (str): The column on the right DataFrame with which to replace the
            attributes.

    Note: This function presents its io side effects proudly. Hows this for your functional
    programming.
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(MergeReplaceFK, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Reads, merges, and replaces attributes with foreign key references."""

        io = CsvIO()
        left = io.read(filepath=self._params["left"], header=0)
        right = io.read(filepath=self._params["right"], header=0)

        df = pd.merge(left=left, right=right, how="left", on=self._params["merge_on"], copy=False)
        df.drop(columns=self._params["replace_left"], inplace=True)

        io.write(data=df, filepath=self._params["destination"], sep=",", header=True)

        return df


# ------------------------------------------------------------------------------------------------ #


class ExpandGZ(Operator):
    """Expandes a gzip archive, stores the raw data

    Args:
        task_no (int): Task sequence in dag.
        task_name (str): name of task
        params (dict): Parameters required by the task, including:
          source (str): The source directory containing the gzip files
          destination (str): The directory into which the Expanded data is to be stored
          force (bool): If True, will execute and overwrite existing data.
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(ExpandGZ, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

        self._source = params["source"]
        self._destination = params["destination"]
        self._force = params["force"]

    @operator
    def execute(self, data: Any = None, context: dict = None) -> pd.DataFrame:
        """Executes the Expand operation.

        Args:
            data (pd.DataFrame): None. This method takes no parameter
            context (dict): None. This method takes no parameter
        """

        # Create destination if it doesn't exist
        os.makedirs(self._destination, exist_ok=True)

        # Only runs if destination directory is empty, unless force is True
        if self._destination_empty_or_force():
            filenames = os.listdir(self._source)
            for filename in filenames:
                filepath = os.path.join(self._source, filename)
                tar = tarfile.open(filepath, "r:gz")
                tar.extractall(self._destination)
                tar.close()

    def _destination_empty_or_force(self) -> bool:
        """Returns true if the file doesn't exist or force is True."""
        num_files = len(os.listdir(self._destination))
        return num_files == 0 or self._force
