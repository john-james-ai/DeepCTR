#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepNeuralCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /base.py                                                                              #
# Language : Python 3.10.4                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepNeuralCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Thursday, April 7th 2022, 3:13:25 pm                                                  #
# Modified : Saturday, April 9th 2022, 2:04:18 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Defines the interfaces for classes involved in the construction and implementation of DAGS."""
from abc import ABC, abstractmethod
import importlib
import pandas as pd
from typing import Any

# ------------------------------------------------------------------------------------------------ #


class AbstractDAG(ABC):
    """Abstract base class for directed acyclic graph of operations.

    Args:
        dag_no (str): Identifier for the dag
        dag_description (str): Brief description
        tasks (list): List of tasks to execute

    """

    def __init__(
        self, dag_no: str, dag_name: str, dag_description: str, tasks: list, context: Any = None
    ) -> None:
        self._dag_no = dag_no
        self._dag_description = dag_description
        self._tasks = tasks
        self._context = context

    @abstractmethod
    def run(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class Dag(AbstractDAG):
    """Directed acyclic graph of operations.

    Args:
        dag_no (str): Identifier for the dag
        dag_name (str): name for the dag in lower case, underscore separated
        dag_description (str): Brief description
        tasks (list): List of tasks to execute

    """

    def __init__(
        self, dag_no: str, dag_name: str, dag_description: str, tasks: list, context: Any = None
    ) -> None:
        super(Dag, self).__init__(
            dag_no=dag_no,
            dag_name=dag_name,
            dag_description=dag_description,
            tasks=tasks,
            context=context,
        )

    def run(self) -> None:
        data = None
        for task in self._tasks:
            result = task.execute(data=data, context=self._context)
            data = result if result is not None else data


# ------------------------------------------------------------------------------------------------ #


class DagBuilder:
    """Constructs a DAG from a configuration dictionary

    Args:
        config (dict): Nested dictionary of tasks defined by a dag_no, dag_name,
        dag_description and a nested dictionary of tasks, where each task is defined by:
          task_no: Sequence number of task
          task: Name of the class that executes the task
          module: The module containing the task
          task_name: A name for the task
          task_params: Any parameters required by the task
    """

    def __init__(self, config: dict, context: dict = None) -> None:
        self._config = config
        self._context = context
        self.reset()

    def reset(self) -> None:
        self._dag = None

    @property
    def dag(self) -> Dag:
        dag = self._dag
        self.reset()
        return dag

    def build(self) -> Dag:

        dag_no = self._config["dag_no"]
        dag_name = self._config["dag_name"]
        dag_description = self._config["dag_description"]

        tasks = []

        for _, task_config in self._config["tasks"].items():

            # Create task object from string using importlib
            module = importlib.import_module(name=task_config["module"])
            task = getattr(module, task_config["task"])

            task_instance = task(
                task_no=task_config["task_no"],
                task_name=task_config["task_name"],
                task_description=task_config["task_description"],
                params=task_config["task_params"],
            )

            tasks.append(task_instance)

        self._dag = Dag(
            dag_no=dag_no,
            dag_name=dag_name,
            dag_description=dag_description,
            tasks=tasks,
            context=self._context,
        )

        return self._dag


# ------------------------------------------------------------------------------------------------ #


class Operator(ABC):
    """Abstract class for operator classes

    Args:
        task_no (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        params (Any): Parameters for the task

    """

    def __init__(
        self,
        task_no: int,
        task_name: str,
        task_description: str,
        params: list,
    ) -> None:
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

    @abstractmethod
    def execute(self, data: pd.DataFrame = None, context: dict = None) -> Any:
        pass
