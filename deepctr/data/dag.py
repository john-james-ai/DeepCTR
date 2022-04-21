#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /base.py                                                                              #
# Language : Python 3.10.4                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Thursday, April 7th 2022, 3:13:25 pm                                                  #
# Modified : Thursday, April 21st 2022, 1:14:03 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Defines the interfaces for classes involved in the construction and implementation of DAGS."""
from abc import ABC, abstractmethod
import os
import importlib
from dotenv import load_dotenv
from pprint import pprint

from deepctr.utils.io import YamlIO


# ------------------------------------------------------------------------------------------------ #


class Context:
    """DAG Context object containing credentials for resources shared among tasks w/in DAG

    The underlying context data structure is a nested dictionary as illustrated below:

    context = {
        'database': {
            'mysql': {
                user: username,
                pwd: somepwd,
                ...
            },
            'somedb': {
                user: anotheruser,
                pwd: anotherpwd,
                ...
            }
        },
        'cloud': {
            'aws': {
                key: somekey,
                pwd: somepwd
                }
            }
        }
    }
    """

    def __init__(self) -> None:
        self._context = {}

    def add_context(self, resource_type: str, resource: str) -> None:
        """Adds context for a given resource and resource type

        Args:
            resource_type (str): Type of resource, i.e. 'database' or 'cloud'
            resource (str): Name of the resource
        """
        # Filepath for credentials is stored in environment variable
        load_dotenv()
        filepath = os.getenv("credentials_filepath")

        io = YamlIO()
        credentials_data = io.read(filepath)
        credentials = credentials_data[resource_type].get(resource)

        self._context[resource_type] = self._context.get(resource_type, {})
        self._context[resource_type][resource] = credentials

    def get_context(self, resource_type: str, resource: str) -> dict:
        """Gets context for a given resource and resource type from context object

        Args:
            resource_type (str): Type of resource, i.e. 'database' or 'cloud'
            resource (str): Name of the resource
        """
        return self._context[resource_type].get(resource)

    def print_context(self) -> None:
        pprint(self._context)


# ------------------------------------------------------------------------------------------------ #


class AbstractDAG(ABC):
    """Abstract base class for directed acyclic graph of operations.

    Args:
        dag_no (str): Identifier for the dag
        dag_description (str): Brief description
        tasks (list): List of tasks to execute

    """

    def __init__(
        self, dag_no: str, dag_name: str, dag_description: str, tasks: list, context: Context = None
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
        self, dag_no: str, dag_name: str, dag_description: str, tasks: list, context: Context = None
    ) -> None:
        super(Dag, self).__init__(
            dag_no=dag_no,
            dag_name=dag_name,
            dag_description=dag_description,
            tasks=tasks,
            context=context,
        )

    def run(self, start: int = 0, stop: float = float("inf")) -> None:
        data = None
        for task in self._tasks:
            if task.task_no >= start and task.task_no <= stop:
                result = task.execute(data=data, context=self._context)
                data = result if result is not None else data


# ------------------------------------------------------------------------------------------------ #


class DagBuilder:
    """Constructs a pipeline, a.k.a. a directed acyclic graph (DAG)"""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._dag = None

    @property
    def dag(self) -> Dag:
        dag = self._dag
        self.reset()
        return dag

    def build(self, config: dict) -> Dag:

        dag_no = config["dag_no"]
        dag_name = config["dag_name"]
        dag_description = config["dag_description"]

        context = self._build_context(config)

        tasks = self._build_tasks(config)

        self._dag = Dag(
            dag_no=dag_no,
            dag_name=dag_name,
            dag_description=dag_description,
            tasks=tasks,
            context=context,
        )

        return self._dag

    def _build_context(self, config: dict = None) -> dict:
        """Builds context for the dag 'resources'"""

        context = Context()
        resources = config.get("resources", None)

        if resources:
            for resource_type, resources in resources.items():
                for resource in resources:
                    context.add_context(resource_type, resource)
        return context

    def _build_tasks(self, config: dict = None) -> list:
        """Iterates through task and returns a list of task objects."""

        tasks = []

        for _, task_config in config["tasks"].items():

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


class DagRunner:
    """Builds and executes a DAG"""

    def run(self, config_filepath: str) -> None:
        """Builds and executes a DAG

        Args:
            config_filepath (str): Path to file containing the DAG configuration.
        """

        yaml = YamlIO()
        config = yaml.read(config_filepath)

        builder = DagBuilder()
        dag = builder.build(config=config)
        dag.run()
