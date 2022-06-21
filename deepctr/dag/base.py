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
from datetime import datetime
from typing import Any

from deepctr.dal.context import Context

# ------------------------------------------------------------------------------------------------ #


class Operator(ABC):
    """Abstract class for operator classes

    Args:
        seq (int): A number, typically used to indicate the sequence of the task within a DAG
        name (str): String name
        desc (str): A desc for the task
        params (Any): Parameters for the task

    """

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        self._seq = seq
        self._name = name
        self._desc = desc
        self._params = params
        self._created = datetime.now()
        self._task_id = None  # This is the id assigned by the database
        self._started = None
        self._stopped = None
        self._context = None

    def __str__(self) -> str:
        return str(
            "Task #: {}\tTask name: {}\tTask Description: {}\tParams: {}".format(
                self._seq, self._name, self._desc, self._params
            )
        )

    def run(self, data: Any = None, context: Context = None) -> Any:
        self._context = context
        self._setup()
        data = self.execute(data=data, context=context)
        self._teardown()
        return data

    @abstractmethod
    def execute(self, data: Any = None, context: Context = None) -> Any:
        pass

    @property
    def seq(self) -> int:
        return self._seq

    @property
    def name(self) -> str:
        return self._name

    @property
    def desc(self) -> str:
        return self._desc

    @property
    def params(self) -> Any:
        return self._params

    @property
    def created(self) -> datetime:
        return self._created

    @property
    def start(self) -> datetime:
        return self._start

    @property
    def stop(self) -> datetime:
        return self._stop

    @property
    def duration(self) -> datetime:
        return self._duration

    def _start(self) -> None:
        self._started = datetime.now()
        dao = self._context.task
