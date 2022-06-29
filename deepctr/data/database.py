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
# Modified   : Tuesday June 28th 2022 06:41:28 pm                                                  #
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

    def __init__(self, database: str = None) -> None:
        self._database = database
        self._connection = None

    def get_connection(self) -> pymysql.connect:
        if self._connection is None:
            return self._create()
        elif self._connection.open:
            return self._connection
        else:
            self._connection.close()
            return self._create()

    def _create(self) -> pymysql.connect:

        load_dotenv()
        host = os.getenv("HOST")
        user = os.getenv("USER")
        password = os.getenv("PASSWORD")
        # port = os.getenv("PORT")
        database = self._database if self._database is not None else os.getenv("DATABASE")

        try:
            self._connection = pymysql.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                cursorclass=pymysql.cursors.DictCursor,
                charset="utf8mb4",
            )
            logger.info("Database {} opened by {}".format(database, user))
        except pymysql.MySQLError as e:
            logger.error("Execute error %d: %s" % (e.args[0], e.args[1]))
            raise ConnectionError(e)
        return self._connection


# ------------------------------------------------------------------------------------------------ #
class Database:
    """Class responsible for direct access to database."""

    def __init__(self, connection: pymysql.connections.Connection) -> None:
        self._connection = connection

    def _insert(self, statement, parameters=None):
        try:
            cursor = self._connection.cursor()
            cursor.execute(statement, parameters)
            statement = "SELECT LAST_INSERT_ID();"
            return cursor.execute(statement)
        except pymysql.err.MySQLError as e:
            logger.error("Execute error %d: %s" % (e.args[0], e.args[1]))

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

    def select(self, statement: str, parameters: tuple = None, todf: bool = False):
        """Select query that returns mulltiple rows.

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

    def select_all(self, statement: str, parameters: tuple = None, todf: bool = False):
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

    def exists(self, statement: str, parameters: tuple = None, id: bool = False) -> bool:
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
        if id:
            return tuple(cursor.fetchone().items())[0][1]
        else:
            return 1 == tuple(cursor.fetchone().items())[0][1]

    def insert(self, statement: str, parameters: tuple = None):
        """Executes an insert command and returns the last inserted primary key.

        Args:
            statement (str): The SQL query statement
            parameters (tuple): The parameters for the query

        Returns:
            Single row of data

        Raises:
            MySQL Error if execute is not successful.
        """

        return self._insert(statement=statement, parameters=parameters)

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

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()
