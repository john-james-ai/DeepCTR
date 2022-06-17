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

from deepctr.dal.dto import TaskDTO
from deepctr.utils.decorators import tracer
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
        self._start = None
        self._stop = None
        self._task_orm = None
        self._context = None

    def __str__(self) -> str:
        return str(
            "Task #: {}\tTask name: {}\tTask Description: {}\tParams: {}".format(
                self._seq, self._name, self._desc, self._params
            )
        )

    def run(self, data: Any = None, context: Context = None) -> Any:
        self._context = context
        self.create_task_orm()
        self.start_task_orm()
        data = self.execute(data=data, context=context)
        self.stop_task_orm()
        return data

    @abstractmethod
    def execute(self, data: Any = None) -> Any:
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

    @tracer
    def create_task_orm(self) -> None:

        # Obtaining the DAG ORM object from context for its dag_id
        dag = self._context.dag
        # Create Data Transfer Object carrying the task information
        dto = TaskDTO(
            seq=self._seq, name=self._name, desc=self._desc, dag_id=dag.id, created=self._created,
        )
        # Convert the DTO into a TaskORM object
        self._task_orm = self._context.tasks.create(data=dto)
        # Add the task to the context
        self._context.task = self._task_orm

    @tracer
    def start_task_orm(self) -> None:
        # Starting the TaskORM object sets the start time
        self._task_orm = self._context.tasks.start(task=self._task_orm)
        # Add the task to the database
        self._task_orm = self._context.tasks.add(task=self._task_orm)
        # Set the property on the task object
        self._start = self._task_orm.start
        # Update the task on the context
        self._context.task = self._task_orm

    @tracer
    def stop_task_orm(self) -> None:
        # Stopping the TaskORM object sets the stop time and computes task duration
        self._task_orm = self._context.tasks.stop(task=self._task_orm)
        # Update the database
        self._task_orm = self._context.tasks.add(task=self._task_orm)
        # Set the stop time and duration on the task properties
        self._stop = self._task_orm.stop
        self._duration = self._task_orm.duration
        # Final update of the task on the context
        self._context.task = self._task_orm
