#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /database.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 19th 2022 06:39:17 pm                                                  #
# Modified   : Monday May 23rd 2022 06:18:49 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
import os
import logging
from dotenv import load_dotenv
import pymysql

from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DBConfig:
    """Encapsulates MySQL database credentials"""

    load_dotenv()
    host = os.getenv("HOST")
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    port = os.getenv("PORT")
    database = os.getenv("DATABASE")


# ------------------------------------------------------------------------------------------------ #


class ConnectionFactory:
    """MySQL database connections context manager."""

    def get_connection(self, database: str = "deepctr", config: DBConfig = DBConfig()) -> None:
        try:
            return pymysql.connect(
                host=config.host,
                user=config.user,
                password=config.password,
                database=database,
                charset="utf8mb4",
            )
        except pymysql.err.MySQLError as e:
            logger.error("Execute error %d: %s" % (e.args[0], e.args[1]))


# ------------------------------------------------------------------------------------------------ #
class Database:
    """Class responsible for direct access to database."""

    def __init__(self, connection: pymysql.connections.Connection) -> None:
        self._connection = connection

    def _execute(self, statement, parameters=None):
        try:
            cursor = self._connection.cursor()
            return cursor.execute(statement, parameters)
        except pymysql.err.MySQLError as e:
            logger.error("Execute error %d: %s" % (e.args[0], e.args[1]))

    def _query(self, statement, parameters=None):
        try:
            cursor = self._connection.cursor()
            cursor.execute(statement, parameters)
            return cursor
        except pymysql.err.MySQLError as e:
            logger.error("Execute error %d: %s" % (e.args[0], e.args[1]))

    def select(self, statement: str, parameters: tuple = None):
        """Select query that returns many rows.

        Args:
            statement (str): The SQL query statement
            parameters (tuple): The parameters for the query

        Returns:
            Single row of data

        Raises:
            MySQL Error if execute is not successful.
        """
        cursor = self._query(statement=statement, parameters=parameters)
        return cursor.fetchall()

    def select_all(self, statement: str, parameters: tuple = None):
        """Select query that returns many rows. Alias for select.

        Args:
            statement (str): The SQL query statement
            parameters (tuple): The parameters for the query

        Returns:
            Single row of data

        Raises:
            MySQL Error if execute is not successful.
        """
        return self.select(statement=statement, parameters=parameters)

    def select_one(self, statement: str, parameters: tuple = None):
        """Select query that returns one row.

        Args:
            statement (str): The SQL query statement
            parameters (tuple): The parameters for the query

        Returns:
            Single row of data

        Raises:
            MySQL Error if execute is not successful.
        """
        cursor = self._query(statement=statement, parameters=parameters)
        return cursor.fetchone()

    def select_exists(self, statement: str, parameters: tuple = None):
        """Select query that returns one row.

        Args:
            statement (str): The SQL query statement
            parameters (tuple): The parameters for the query

        Returns:
            Single row of data

        Raises:
            MySQL Error if execute is not successful.
        """
        cursor = self._query(statement=statement, parameters=parameters)
        return cursor.fetchone()[0]

    def execute(self, statement: str, parameters: tuple = None):
        """Executes a command that changes the data.

        Args:
            statement (str): The SQL query statement
            parameters (tuple): The parameters for the query

        Returns:
            Single row of data

        Raises:
            MySQL Error if execute is not successful.
        """

        return self._execute(statement=statement, parameters=parameters)
