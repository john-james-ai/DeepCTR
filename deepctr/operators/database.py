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
# Modified : Thursday, April 21st 2022, 2:49:35 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Tasks that complete the Load phase of the ETL DAG"""
import logging
from pymysql import connect
from pymysql.cursors import DictCursor
import sqlalchemy
from sqlalchemy.types import Integer, BigInteger, Float, String  # noqa: F401
from typing import Any

from deepctr.data.dag import Context
from deepctr.operators.base import Operator
from deepctr.utils.decorators import operator

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                 DATABASE FACTORY CLASS                                           #
# ------------------------------------------------------------------------------------------------ #


class SQLEngine(Operator):
    """Executes SQL Statements

    Args:
        task_id: Sequence number for the task in its dag
        task_name (str): String name
        task_description (str): A description for the task
        params: The parameters the task requires

    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(SQLEngine, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: Context = None) -> None:

        connection = self._get_connection(context=context)

        with connection.cursor() as cursor:
            cursor.execute(self._params["query"])
            connection.commit()

        connection.close()

    def _get_connection(self, context: Context) -> connect:
        """Return a MySQL database connection."""

        credentials = self.get_credentials(
            external_resource=self._params["external_resource"], context=context
        )

        connection = connect(
            host=credentials["host"],
            user=credentials["user"],
            password=credentials["password"],
            database=credentials["database"],
            charset="utf8",
            cursorclass=DictCursor,
        )
        return connection


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
    def execute(self, data: Any = None, context: Context = None) -> None:

        engine = self._get_engine(context=context)

        # Identify columns and convert strings to sqlalchemy datatypes
        columns = []
        dtypes = self._params["dtypes"]
        for column, dtype in self._params["dtypes"].items():
            dtypes[column] = eval(dtype)
            columns.append(column)

        # Extract columns of interest
        data = data[columns]

        # 4/20/2022: Setting chunksize to 10,000
        # Source: https://acepor.github.io/2017/08/03/using-chunksize/
        data.to_sql(
            name=self._params["table"],
            chunksize=10000,
            con=engine,
            method="multi",
            index=False,
            if_exists="append",
            dtype=dtypes,
        )

    def _get_engine(self, context: dict) -> sqlalchemy.engine:
        """Return an SQLAlchemy Database Engine"""

        credentials = self.get_credentials(
            external_resource=self.params["external_resource"], context=context
        )

        engine = sqlalchemy.create_engine(credentials["uri"])
        return engine
