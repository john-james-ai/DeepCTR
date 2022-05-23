#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /context.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 22nd 2022 12:30:45 am                                                    #
# Modified   : Sunday May 22nd 2022 12:30:45 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset context object that implements the Repository/Unit of Work Pattern."""
import logging
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class DatasetContext:
    """Dataset context used to query and persist the underlying database."""
    # Needs to be a context class. The creation of entities is performed by
    # the DBSet classes, who control the table operations. The DBSet
    # contains the SQL for CRUD. Using the context, invoke, the DBset objects'
    # create methods, then execute the saveChanges method on the context.
    # which invokes the appropriate method on the DBset that generates
    # the SQL and performs the database command.

    # Though entities may be composed. they are not stored in the
    # DBSet as compositions.

    def __init__(self) -> None:
        self._connection = None
        self._files = None
        self._datasets = None

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, connection):
        self._connection = connection

    @property
    def files(self):
        return self._files

    @files.setter
    def files(self, files):
        self._files = files

    @property
    def datasets(self):
        return self._datasets

    @datasets.setter
    def datasets(self, datasets):
        self._datasets = datasets


    def add(self, dataset) -> None:
        with self._connection() as connection:

        self._


