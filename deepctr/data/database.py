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
# Modified   : Friday May 27th 2022 05:29:25 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
import os
import logging
import logging.config
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
            connection = pymysql.connect(
                host=config.host,
                user=config.user,
                password=config.password,
                database=database,
                cursorclass=pymysql.cursors.DictCursor,
                charset="utf8mb4",
            )
            logger.info("Database {} opened by {}".format(database, config.user))
            return connection
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

    def select_one(self, statement: str, parameters: tuple = None) -> dict:
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

    def exists(self, statement: str, parameters: tuple = None) -> bool:
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
        return 1 == tuple(cursor.fetchone().items())[0][1]

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

    def begin_transaction(self) -> None:
        self._connection.begin()

    def rollback(self) -> None:
        self._connection.rollback()

    def save(self) -> None:
        self._connection.commit()
