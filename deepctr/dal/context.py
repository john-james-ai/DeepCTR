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
# Modified   : Monday June 20th 2022 02:21:46 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset context object that implements the Repository/Unit of Work Pattern."""
from abc import ABC, abstractmethod
import logging
from pymysql.connections import Connection


from deepctr.dal.dao import LocalFileDAO, S3FileDAO, DagDAO, TaskDAO
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Context(ABC):
    """Base class defining the interface for all context objects.

    Context controls the database connection and transactions, as well as the
    data access objects (DAOs), one for each table.

    """

    def __init__(self, database: str = "deepctr") -> None:
        self._database = database
        self._connection = None

    def connect(self):
        load_dotenv()
        host = os.getenv("HOST")
        user = os.getenv("USER")
        password = os.getenv("PASSWORD")
        port = os.getenv("PORT")
        database = os.getenv("DATABASE")
        try:
            self._connection = pymysql.connect(
                host=config.host,
                user=config.user,
                password=config.password,
                database=database,
                cursorclass=pymysql.cursors.DictCursor,
                charset="utf8mb4",
            )
        except pymysql.Error as e:
            logger.error("Could not open {} database. Error: {}".format(self._dbname, e))
            raise

    def __exit__(self, exception_type=None, exception_value=None, traceback=None):
        if exception_type is None:
            self.commit()
            self.close()
        else:
            logger.error(
                "Exception type: {}\tException value: {}\n{}".format(
                    exception_type, exception_value, traceback
                )
            )
            self.rollback()
            self.close()
            raise

    @property
    def connection(self):
        return self._connection

    @dag.setter
    def dag(self, dag: DagORM) -> None:
        self._dag = dag

    @property
    def task(self) -> TaskORM:
        return self._task

    @task.setter
    def task(self, task: TaskORM) -> None:
        self._task = task

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @dataset.setter
    def dataset(self, dataset: Dataset) -> None:
        self._dataset = dataset

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def rollback(self) -> None:
        self._connection.rollback()

    @abstractmethod
    def connect(self) -> None:
        """Adds connections to all the data access objects."""
        pass

    @abstractmethod
    def save(self) -> None:
        """Runs save to commit changes on all data access objects."""
        pass


# ------------------------------------------------------------------------------------------------ #
class DataContext(Context):
    """Defines the context for data related DAGs"""

    def __init__(self, connection: Connection) -> None:
        super(DataContext, self).__init__(connection)

    def connect(self) -> None:
        self._localfiles = LocalFileDAO(self._connection)
        self._localdatasets = LocalDatasetDAO(self._connection)
        self._s3files = S3FileDAO(self._connection)
        self._s3datasets = S3DatasetDAO(self._connection)
        self._dags = DagDAO(self._connection)
        self._tasks = TaskDAO(self._connection)

    def save(self) -> None:
        self._localfiles.save()
        self._localdatasets.save()
        self._s3files.save()
        self._s3datasets.save()
        self._dags.save()
        self._tasks.save()
