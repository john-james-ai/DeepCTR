#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /base.py                                                                              #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, April 16th 2022, 7:00:56 am                                                 #
# Modified : Monday, April 25th 2022, 3:46:35 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import Any

# ------------------------------------------------------------------------------------------------ #


class Operator(ABC):
    """Abstract class for operator classes

    Args:
        task_no (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        task_description (str): A description for the task
        params (Any): Parameters for the task

    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: dict) -> None:
        self._task_no = task_no
        self._task_name = task_name
        self._task_description = task_description
        self._params = params

    def __str__(self) -> str:
        return str(
            "Task #: {}\tTask name: {}\tTask Description: {}\tParams: {}".format(
                self._task_no, self._task_name, self._task_description, self._params
            )
        )

    @abstractmethod
    def execute(self, data: Any = None) -> Any:
        pass

    @property
    def task_no(self) -> int:
        return self._task_no

    @property
    def task_name(self) -> str:
        return self._task_name

    @property
    def task_description(self) -> str:
        return self._task_description

    @property
    def params(self) -> Any:
        return self._params
