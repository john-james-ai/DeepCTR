#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /data_operators.py                                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 10th 2022 03:30:15 pm                                                   #
# Modified   : Tuesday May 17th 2022 05:52:47 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from typing import Any
import logging
import logging.config
import pandas as pd

from deepctr.utils.decorators import operator
from deepctr.dag.base import Operator
from deepctr.dal.params import DatasetParams, DatasetParams, S3Params
from deepctr.dal.files import FileAccessObject, RemoteAccessObject
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                     DOWNLOAD S3                                                  #
# ------------------------------------------------------------------------------------------------ #


class DownloadS3(Operator):
    """Operator that downloads data from Amazon S3 Resources.

    Args:
        task_no (int): Task sequence in dag.
        task_name (str): name of task
        params (dict): Parameters required by the task, including:
          bucket (str): The Amazon S3 bucket name
          key (str): The access key to the S3 bucket
          password (str): The secret access key to the S3 bucket
          folder (str): The folder within the bucket for the data
          destination (str): The folder to which the data is downloaded
          force (bool): If True, will execute and overwrite existing data.
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: dict) -> None:
        super(DownloadS3, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

        self._progressbar = None

    @operator
    def execute(self, data: Any = None) -> pd.DataFrame:
        """Extracts data from an Amazon AWS S3 resource and persists it."""

        source = S3Params(
            bucket=self._params["source"]["bucket"], folder=self._params["source"]["folder"]
        )

        destination = DatasetParams(
            datasource=self._params["destination"]["datasource"],
            dataset=self._params["destination"]["dataset"],
            stage=self._params["destination"]["stage"],
            home=self._params["destination"]["home"],
        )

        rao = RemoteAccessObject()
        rao.download_dataset(source=source, destination=destination, expand=True, force=False)


# ------------------------------------------------------------------------------------------------ #
#                                     DATA READER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DataReader(Operator):
    """Reads data from the Data Repository"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(DataReader, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None) -> Any:
        """Reads from the designated resource"""

        params = DatasetParams(
            datasource=self._params["datasource"],
            dataset=self._params["dataset"],
            stage=self._params["stage"],
            home=self._params["home"],
            filename=self._params["filename"],
            format=self._params["format"],
        )

        return FileAccessObject().read(params=params)


# ------------------------------------------------------------------------------------------------ #
#                                     DATA WRITER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DataWriter(Operator):
    """Reads data from the Data Repository"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(DataWriter, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None) -> Any:
        """Reads from the designated resource"""

        params = FileAccessObject(
            datasource=self._params["datasource"],
            dataset=self._params["dataset"],
            filename=self._params["filename"],
            format=self._params["format"],
            stage=self._params["stage"],
            home=self._params["home"],
        )

        return FileAccessObject().create(params=params, data=data)
