#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_metadata_operators.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 27th 2022 07:11:18 am                                                    #
# Modified   : Saturday May 28th 2022 06:27:39 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from typing import Any
from abc import abstractmethod
from datetime import datetime
import logging
import logging.config
import pandas as pd

from deepctr.dal.entity import Dataset, File
from deepctr.utils.decorators import operator
from deepctr.dag.base import Operator
from deepctr.dal.context import Context

from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
class Handler(Operator):
    """Test Operator"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        self._seq = seq
        self._name = name
        self._desc = desc
        self._params = params

    @abstractmethod
    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        pass


# ------------------------------------------------------------------------------------------------ #
class DatasetHandler(Handler):
    """Test Operator"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(DatasetHandler, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        logger.info("Processing Dataset Started at {}.".format(datetime.now()))

        try:
            for _, dataset in data.items():
                self._handle_dataset(dataset, context)
        except KeyError as e:
            logger.error("Invalid configuration file. Data not found.\n{}".format(e))
            raise ValueError(e)

        logger.info("Processing Dataset Completed at {}.".format(datetime.now()))

    def _handle_dataset(self, dataset: Dataset, context: Context) -> None:

        # Create the Dataset object
        dataset = context.datasets.create(data=dataset)

        # Update the dataset with dag and task id's from context if exists
        dataset.dag_id = context.dag.id or 0
        dataset.task_id = context.task.id or 0

        # Check if the dataset already exists, if it does, get the id and update the Dataset
        id = context.datasets.exists(dataset=dataset, id=True)
        if id:
            dataset.id = id
            dataset = context.datasets.update(dataset=dataset)
        # Otherwise add the Dataset
        else:
            dataset = context.dataset.add(dataset=dataset)

        # Add the dataset to context.
        context.dataset = dataset


# ------------------------------------------------------------------------------------------------ #
class LocalFileHandler(Handler):
    """Handles local file registration"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(DatasetHandler, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        logger.info("Processing LocalFile Started at {}.".format(datetime.now()))

        try:
            for _, file in data.items():
                self._handle_file(file, context)
        except KeyError as e:
            logger.error("Invalid configuration file. File not found.\n{}".format(e))
            raise ValueError(e)

        logger.info("Processing Dataset Completed at {}.".format(datetime.now()))

    def _handle_file(self, file: File, context: Context) -> None:

        # Create the Dataset object
        file = context.localfiles.create(data=file)

        # Update the dataset with dag and task id's from context if exists
        file.dag_id = context.dag.id or 0
        file.task_id = context.task.id or 0

        # Check if the file already exists, if it does, get the id and update the file
        id = context.localfiles.exists(file=file, id=True)
        if id:
            file.id = id
            file = context.localfiles.update(file=file)
        # Otherwise add the file
        else:
            file = context.localfiles.add(file=file)


# ------------------------------------------------------------------------------------------------ #
class S3FileHandler(Handler):
    """Handles s3 file registration"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(DatasetHandler, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        logger.info("Processing LocalFile Started at {}.".format(datetime.now()))

        try:
            for _, file in data.items():
                self._handle_file(file, context)
        except KeyError as e:
            logger.error("Invalid configuration file. File not found.\n{}".format(e))
            raise ValueError(e)

        logger.info("Processing LocalFile Completed at {}.".format(datetime.now()))

    def _handle_file(self, file: File, context: Context) -> None:

        # Create the Dataset object
        file = context.s3files.create(data=file)

        # Update the dataset with dag and task id's from context if exists
        file.dag_id = context.dag.id or 0
        file.task_id = context.task.id or 0

        # Check if the file already exists, if it does, get the id and update the file
        id = context.s3files.exists(file=file, id=True)
        if id:
            file.id = id
            file = context.s3files.update(file=file)
        # Otherwise add the file
        else:
            file = context.s3files.add(file=file)
