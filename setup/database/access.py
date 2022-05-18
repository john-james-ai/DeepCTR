#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /DataAccessObject.py                                                                               #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Thursday, April 21st 2022, 2:08:33 am                                                 #
# Modified : Wednesday, May 4th 2022, 12:41:32 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import pandas as pd
import logging
import logging.config
from dotenv import load_dotenv
from sqlalchemy import create_engine

from deepctr.utils.database import Query
from deepctr.utils.decorators import query
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class DataAccessObject:
    """Database Access Object"""

    __connection = None
    __database = None

    def __init__(self, database: str = "mysql"):
        self._connection_check(database)

    def connect(self, database: str = "mysql") -> None:
        """Creates an SQL Alchemy connection to the designated database.

        Args:
            database (str): Name of database to connect
        """

        try:
            load_dotenv()
            URI = os.getenv("URI") + database
            engine = create_engine(URI)
            DataAccessObject.__connection = engine.connect()
            DataAccessObject.__database = database
            logger.debug("Connection to {} established.".format(database))
        except Exception as error:
            logger.error("Error: Connection not established.\n{}".format(error))

    def close(self) -> None:
        """Closes the current connection."""
        DataAccessObject.__connection.close()
        DataAccessObject.__database = None

    @query
    def read(self, query: Query, parameters: tuple = None, columns: list = None) -> pd.DataFrame:
        """Reads SQL query or database table into a DataFrame

        Args:
            query (Query): The query object with the statement to execute
            parameters (tuple): Tuple containing query parameters (optional)
            columns (list): List of columns to include in query

        Returns: pd.DataFrame
        """

        self._connection_check(query.database)  # Ensures a connection

        df = pd.read_sql(
            query.statement,
            con=DataAccessObject.__connection,
            coerce_float=True,
            params=parameters,
            columns=columns,
        )
        return df

    def exists(self, query: Query, parameters: tuple = None) -> bool:
        """Method for existence checks. Returns a boolean

        Args:
            query (Query): The query object with the statement to execute
            parameters (tuple): Tuple containing query parameters (optional)

        Returns: bool

        """

        df = self.read(query=query, parameters=parameters)
        print("*" * 40)
        print(df)
        return query.exists(df)

    def _connection_check(self, database: str = "mysql") -> None:
        if DataAccessObject.__connection is None:
            self.connect(database)
        elif DataAccessObject.__database != database:
            self.close()
            self.connect(database)
