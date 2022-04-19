#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /load.py                                                                              #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 12th 2022, 5:34:59 am                                                 #
# Modified : Sunday, April 17th 2022, 5:25:20 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Tasks that complete the Load phase of the ETL DAG"""
import pandas as pd
import logging
from pymysql import connect
from pymysql.cursors import DictCursor
from typing import Any
import sqlalchemy
from sqlalchemy.types import Integer, BigInteger, Float, String  # noqa: F401

from deepctr.operators.base import Operator
from deepctr.utils.decorators import operator

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                 DATABASE FACTORY CLASS                                           #
# ------------------------------------------------------------------------------------------------ #


class DatabaseFactory(Operator):
    """Creates a Database

    Args:
        task_id: Sequence number for the task in its dag
        task_name (str): String name
        task_description (str): A description for the task
        params: The parameters the task requires

    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(DatabaseFactory, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> None:

        # Obtain a database connection
        connection = connect(
            host=context["HOST"],
            user=context["USER"],
            password=context["PASSWORD"],
            database=context["DATABASE"],
            charset="utf8",
            cursorclass=DictCursor,
        )

        with connection.cursor() as cursor:
            for _, statement in data.items():
                cursor.execute(statement)
            connection.commit()

        connection.close()


# ------------------------------------------------------------------------------------------------ #
#                                     DATABASE LOADER                                              #
# ------------------------------------------------------------------------------------------------ #


class TableLoader(Operator):
    """Loads data into tables."""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(TableLoader, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> None:

        engine = sqlalchemy.create_engine(context["DB_URI"])

        # Identify columns and convert strings to sqlalchemy datatypes
        columns = []
        dtypes = self._params["dtypes"]
        for column, dtype in self._params["dtypes"].items():
            dtypes[column] = eval(dtype)
            columns.append(column)

        # Extract columns of interest
        data = data[columns]

        data.to_sql(
            name=self._params["table"],
            con=engine,
            index=False,
            if_exists="append",
            dtype=dtypes,
        )
