#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /orchestrator.py                                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 10th 2022 03:30:15 pm                                                   #
# Modified   : Friday May 13th 2022 11:40:31 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

"""Defines construction and execution of DAGs."""
from abc import ABC, abstractmethod
import importlib
import logging

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                          DAG                                                     #
# ------------------------------------------------------------------------------------------------ #


class DAG(ABC):
    """Abstract base class for directed acyclic graph of operations.

    Args:
        dag_no (str): Identifier for the dag
        dag_description (str): Brief description
        tasks (list): List of tasks to execute

    """

    def __init__(self, dag_no: str, dag_name: str, dag_description: str, tasks: list) -> None:
        self._dag_no = dag_no
        self._dag_name = dag_name
        self._dag_description = dag_description
        self._tasks = tasks

    @abstractmethod
    def run(self, start: int = 0, stop: float = float("inf")) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class DataDAG(DAG):
    """Directed acyclic graph for data operations.

    Args:
        dag_no (str): Identifier for the dag
        dag_name (str): name for the dag in lower case, underscore separated
        dag_description (str): Brief description
        tasks (list): List of tasks to execute

    """

    def __init__(self, dag_no: str, dag_name: str, dag_description: str, tasks: list) -> None:
        super(DataDAG, self).__init__(
            dag_no=dag_no, dag_name=dag_name, dag_description=dag_description, tasks=tasks,
        )

    def run(self, start: int = 0, stop: float = float("inf")) -> None:
        data = None
        for task in self._tasks:
            if task.task_no >= start and task.task_no <= stop:
                result = task.execute(data=data)
                data = result if result is not None else data


# ------------------------------------------------------------------------------------------------ #
#                                     DAG BUILDERS                                                 #
# ------------------------------------------------------------------------------------------------ #
class DAGBuilder(ABC):
    """Abstract base class for DAG builders """

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._dag = None

    @property
    def dag(self) -> DAG:
        return self._dag

    @abstractmethod
    def build(self, config: dict) -> None:
        pass

    def _build_tasks(self, config: dict = None) -> list:
        """Iterates through task and returns a list of task objects."""

        tasks = []

        for _, task_config in config["tasks"].items():

            # Add context to the parameters dictionary
            task_config["task_params"].update(config["dag_context"])

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

        return tasks


# ------------------------------------------------------------------------------------------------ #


class DataDAGBuilder(DAGBuilder):
    def __init__(self) -> None:
        super(DataDAGBuilder, self).__init__()

    def build(self, config: dict) -> None:

        tasks = self._build_tasks(config)

        try:
            self._dag = DataDAG(
                dag_no=config["dag_no"],
                dag_name=config["dag_name"],
                dag_description=config["dag_description"],
                tasks=tasks,
            )
        except KeyError as e:
            logger.error("Invalid configuration parameters")
            raise ValueError(e)
