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
# Modified   : Monday June 20th 2022 01:16:20 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

"""Defines construction and execution of DAGs."""
from abc import ABC, abstractmethod
from datetime import datetime
import importlib
import logging
import logging.config

from deepctr.dal.context import Context
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
#                                          DAG                                                     #
# ------------------------------------------------------------------------------------------------ #


class DAG(ABC):
    """Abstract base class for directed acyclic graph of operations.

    Args:
        name (str): A brief and unique name for the dag
        dag_desc (str): Brief description
        tasks (list): List of tasks to execute

    """

    def __init__(self, name: str, desc: str, tasks: list, context: Context) -> None:
        self._id = 0
        self._name = name
        self._desc = desc
        self._tasks = tasks
        self._context = context
        self._started = None
        self._stopped = None
        self._duration = None
        self._created = datetime.now()

    def run(self, started: int = 0, stopped: float = float("inf")) -> None:
        self._start()
        self.execute(started=started, stopped=stopped, context=self._context)
        self._stop()

    def _start(self) -> None:
        """Sets start time,  creates the dag db entry, and updates the id from the database."""

    def _insert_dag(self) -> int:
        """Inserts the dag into the database and returns the dag id."""
        dao = self._context.dag


# ------------------------------------------------------------------------------------------------ #


class DataDAG(DAG):
    """Directed acyclic graph for data operations.

    Args:
        seq (str): Identifier for the dag
        dag_name (str): name for the dag in lower case, underscore separated
        dag_desc (str): Brief desc
        tasks (list): List of tasks to execute

    """

    def __init__(self, name: str, desc: str, tasks: list) -> None:
        super(DataDAG, self).__init__(name=name, desc=desc, tasks=tasks)

    def execute(
        self, started: int = 0, stopped: float = float("inf"), context: Context = None
    ) -> None:
        data = None
        with context as c:
            for task in self._tasks:
                if task.seq >= started and task.seq <= stopped:
                    result = task.run(data=data, context=c)
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

            # Create task object from string using importlib
            module = importlib.import_module(name=task_config["module"])
            task = getattr(module, task_config["task"])

            task_instance = task(
                name=task_config["task_name"],
                seq=task_config["task_seq"],
                desc=task_config["task_desc"],
                params=task_config["task_params"],
            )

            tasks.append(task_instance)

        return tasks


# ------------------------------------------------------------------------------------------------ #


class DataDAGBuilder(DAGBuilder):
    def __init__(self) -> None:
        super(DataDAGBuilder, self).__init__()
        self.reset()

    def reset(self) -> None:
        self._dag = None
        self._home = None
        self._datasource = None
        self._dataset = None
        self._config = None
        self._context = None
        return self

    def datasource(self, datasource: str) -> DAGBuilder:
        self._datasource = datasource
        return self

    def dataset(self, dataset: str) -> DAGBuilder:
        self._dataset = dataset
        return self

    def at(self, at: str) -> DAGBuilder:
        self._home = at
        return self

    def with_template(self, template: dict) -> DAGBuilder:
        self._template = template
        return self

    def and_context(self, context: Context) -> None:
        self._context = context
        return self

    def build(self) -> None:

        # Create a configuration from the template
        config = self._create_config()

        # Create the tasks that will be performed on the files.
        tasks = self._build_tasks(config)

        try:
            self._dag = DataDAG(name=config["dag_name"], desc=config["dag_desc"], tasks=tasks,)
        except KeyError as e:
            logger.error("Invalid configuration parameters")
            raise ValueError(e)

        self._dag.context = self._context

        return self

    def _create_config(self) -> dict:
        config = self._template
        return config
