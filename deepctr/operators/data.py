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
# Modified : Monday, April 25th 2022, 5:58:26 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import pandas as pd
from datetime import datetime
from typing import Any
import tarfile
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import timestamp_seconds, year, month, day, hour, dayofmonth, col

from deepctr.utils.decorators import operator
from deepctr.operators.base import Operator
from deepctr.utils.io import CsvIO
from deepctr.data.dag import Context
from deepctr.utils.sample import sample_from_file

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

class TimeStampDecoder(Operator):
    """Extracts Year, Month, Day and Hour from Timestamp and adds values as columns.

    Args:
        task_no (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        task_description (str): A description for the task
        params (Any): Parameters for the task
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(TimeStampDecoder, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        """Extracts temporal data from timestamp and adds as columns to data."""

        # Extract the name of the timestamp column from the parameter list
        timestamp_var = self._params.get('timestamp_var',None)
        
        # Extract year, month and monthday from the data
        yr = year(timestamp_seconds(col(timestamp_var)))
        mth = month(timestamp_seconds(col(timestamp_var)))
        mthday = dayofmonth(timestamp_seconds(col(timestamp_var)))
        
        # Add date data as columns.
        data = (data
        .withColumn("year", yr)
        .withColumn("month", mth)
        .withColumn("day",mthday)        
        )

        return data

    

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
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        """Replaces the columns in the DataFrame according to the params['columns'] object."""

        # Dispatch to the appropriate Pandas or Spark operator for the column processing.
        if isinstance(data, pd.DataFrame):

            data = self._rename_pandas_cols(data)  

        else:
            data = self._rename_spark_cols(data)

        return data

    def _rename_pandas_cols(self, data: pd.DataFrame) -> pd.DataFrame:

        data.columns = data.columns.str.replace(" ", "")  # Remove any whitespace in column names
        data.rename(columns=self._params["columns"], inplace=True)
        return data

    def _rename_spark_cols(self, data: DataFrame) -> DataFrame:

        data = data.toDF(*[x for x in self._params['columns'].values]) 
        return data



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


# ------------------------------------------------------------------------------------------------ #


class AlibabaDevSet(Operator):
    """Creates a development sets for the impression, user,  ad, and behavior datasets.

    Args:
        task_no (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        task_description (str): A description for the task
        params (Any): Parameters for the task including:
            dataset (str): The name of the development dataset to create
            source (str): Directory of source files
            destination (str): Directory of destination files
            sample_size (int): Number of impressions in the development set. Only used with
                impression dataset.
            header (bool): True if data has a header, false otherwise. Default is True
            filename (dict): filename of dataset
            random_state (int): Seed for pseudo random number generation. Only used with
                impression dataset
            force (bool): Only executes if no development set already exists, unless force is True

    Note: This operator has IO side effects. Deal with it.

    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(AlibabaDevSet, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )
        self._start = None

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        """Creates development for the dataset passed named in the params variable.

        This method controls the processes by which the development set is created. It supports
        all four datasets: impression, ad, user, and behavior.

            impression: The first instantiation randomly samples a specified number of
                rows fromthe impressions table. It is returned and services as input for
                the next instantiation.
            user: This second instantiation receives the impression data and selects the
                rows from the user file that match on user in the impression file.
            ad: This third instantiation receives the impression data and selects the
                rows from the ad file that match on ad_group_id in the impression file.
            behavior: The last instantiation receives the impression file and selects
                the rows from the behavior table that match on user and represent the
                same time period from which the impression samples were drawn.

        Args:
            data (pd.DataFrame or None): This is None for the impression dataset. For the other
                three datasets, this is the impression data which serves as the basis
                for selecting rows from each of the other three datasets.
            context (dict): Ignored for this operator

        """

        "Skip if data already exists unless force is True"
        if (
            os.path.exists(os.path.join(self._params["destination"], self._params["filename"]))
            and not self._params["force"]
        ):

            logger.info("Alibaba development set step skipped. Development set already exists.")
        else:
            if "imp" in self._params["dataset"]:
                return self._sample_impressions()
            elif "user" in self._params["dataset"]:
                self._sample_user(impressions=data)
            elif "ad" in self._params["dataset"]:
                self._sample_ad(impressions=data)
            else:
                self._sample_behavior(impressions=data)
        return data

    def _sample_impressions(self) -> pd.DataFrame:
        """Creates impressions development set."""

        self._start_message(dataset="impression")

        source_filepath = os.path.join(self._params["source"], self._params["filename"])
        destination_filepath = os.path.join(self._params["destination"], self._params["filename"])

        impression = sample_from_file(
            source=source_filepath,
            size=self._params["sample_size"],
            header=self._params["header"],
            random_state=self._params["random_state"],
        )

        io = CsvIO()
        io.write(
            impression, filepath=destination_filepath, header=self._params["header"], index=False
        )

        self._end_message(dataset="impression")
        return impression

    def _sample_user(self, impressions: pd.DataFrame) -> None:
        """Creates user development set."""

        self._start_message(dataset="user")

        source_filepath = os.path.join(self._params["source"], self._params["filename"])
        destination_filepath = os.path.join(self._params["destination"], self._params["filename"])

        io = CsvIO()

        user = io.read(filepath=source_filepath, header=self._params["header"])
        user = user.loc[user["userid"].isin(impressions["user"])]
        io.write(user, filepath=destination_filepath, header=self._params["header"])

        self._end_message(dataset="user")

    def _sample_ad(self, impressions: pd.DataFrame) -> None:
        """Creates user development set."""

        self._start_message(dataset="ad")

        source_filepath = os.path.join(self._params["source"], self._params["filename"])
        destination_filepath = os.path.join(self._params["destination"], self._params["filename"])

        io = CsvIO()
        ad = io.read(filepath=source_filepath, header=self._params["header"])
        ad = ad.loc[ad["adgroup_id"].isin(impressions["adgroup_id"])]
        io.write(ad, filepath=destination_filepath, header=self._params["header"])

        self._end_message(dataset="ad")

    def _sample_behavior(self, impressions: pd.DataFrame) -> None:
        """Creates user development set."""

        self._start_message(dataset="behavior")

        source_filepath = os.path.join(self._params["source"], self._params["filename"])
        destination_filepath = os.path.join(self._params["destination"], self._params["filename"])

        begin_date = impressions["time_stamp"].min()
        end_date = impressions["time_stamp"].max()

        io = CsvIO()
        behavior = io.read(filepath=source_filepath, header=self._params["header"])
        behavior = behavior.loc[
            (behavior["user"].isin(impressions["user"]))
            & (behavior["time_stamp"] >= begin_date)
            & (behavior["time_stamp"] <= end_date)
        ]
        io.write(behavior, filepath=destination_filepath, header=self._params["header"])

        self._end_message(dataset="behavior")

    def _start_message(self, dataset: str) -> None:
        self._start = datetime.now()
        start_date = self._start.strftime("%A, %B %d %Y")
        start_time = self._start.strftime("I:%M%p")
        logger.info(
            "Started construction of {} development dataset on {} at {}".format(
                dataset, start_date, start_time
            )
        )

    def _end_message(self, dataset: str) -> None:
        end = datetime.now()
        duration = round((end - self._start).total_seconds(), 2)
        end_date = end.strftime("%A, %B %d %Y")
        end_time = end.strftime("I:%M%p")
        logger.info(
            "Completed construction of {} development dataset on {} at {}\tDuration: {}".format(
                dataset, end_date, end_time, str(duration)
            )
        )
