#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepNeuralCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /data.py                                                                              #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepNeuralCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, April 16th 2022, 12:50:46 am                                                #
# Modified : Monday, April 25th 2022, 5:58:26 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import pandas as pd
from typing import Any
import logging
import logging.config

from deepctr.utils.decorators import operator
from deepctr.dag.base import Operator
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class ReplaceColumnNames(Operator):
    """Replace column names in a DataFrame.

    Args:
        task_no (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        task_description (str): A description for the task
        params (Any): Parameters for the task
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(ReplaceColumnNames, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None, context: dict = None) -> pd.DataFrame:
        """Replaces the columns in the DataFrame according to the params['columns'] object."""

        data = data.toDF(*[x for x in self._params["columns"].values()])

        return data
