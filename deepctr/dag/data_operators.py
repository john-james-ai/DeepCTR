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
# Modified   : Monday June 27th 2022 05:15:28 am                                                   #
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
from deepctr.dal.entity import File, Dataset
from deepctr.dal.vfs import FileManager
from deepctr.dal.remote import RemoteAccessObject
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
        seq (int): Task sequence in dag.
        name (str): name of task
        params (dict): Parameters required by the task, including:
          bucket (str): The Amazon S3 bucket name
          key (str): The access key to the S3 bucket
          password (str): The secret access key to the S3 bucket
          folder (str): The folder within the bucket for the data
          destination (str): The folder to which the data is downloaded
          force (bool): If True, will execute and overwrite existing data.
    """

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(DownloadS3, self).__init__(seq=seq, name=name, desc=desc, params=params)

        self._progressbar = None

    @operator
    def execute(self, data: Any = None) -> pd.DataFrame:
        """Extracts data from an Amazon AWS S3 resource and persists it."""

        source = {
            "bucket": self._params["source"]["bucket"],
            "folder": self._params["source"]["folder"],
        }

        destination = Dataset(
            name=self._params["destination"]["dataset"],
            stage=self._params["destination"]["stage"],
            datasource=self._params["destination"]["datasource"],
            home=self._params["destination"]["home"],
        )

        rao = RemoteAccessObject()
        rao.download_dataset(source=source, destination=destination, expand=True, force=False)


# ------------------------------------------------------------------------------------------------ #
#                                     DATA READER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DataReader(Operator):
    """Reads data from the Data Repository"""

    def __init__(self, seq: int, name: str, desc: str, params: list) -> None:
        super(DataReader, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None) -> Any:
        """Reads from the designated resource"""

        file = File(
            name=self._params["file"]["name"],
            dataset=self._params["file"]["dataset"],
            datasource=self._params["file"]["datasource"],
            stage=self._params["file"]["stage"],
            format=self._params["file"]["format"],
            compressed=self._params["file"]["compressed"],
            home=self._params["file"].get("home", "data"),
        )

        return FileManager().read(file=file)


# ------------------------------------------------------------------------------------------------ #
#                                     DATA WRITER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DataWriter(Operator):
    """Reads data from the Data Repository"""

    def __init__(self, seq: int, name: str, desc: str, params: list) -> None:
        super(DataWriter, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None) -> Any:
        """Reads from the designated resource"""

        file = File(
            name=self._params["file"]["name"],
            dataset=self._params["file"]["dataset"],
            datasource=self._params["file"]["datasource"],
            stage=self._params["file"]["stage"],
            format=self._params["file"]["format"],
            compressed=self._params["file"]["compressed"],
            home=self._params["file"].get("home", "data"),
            file_system=self._params["file"]["file_system"],
        )

        return FileManager().create(file=file, data=data, force=self._params.get("force", False))
