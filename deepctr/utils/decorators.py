#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deepctr: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /decorators.py                                                                        #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, March 14th 2022, 7:53:27 pm                                                   #
# Modified : Thursday, April 21st 2022, 12:20:21 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import functools
from datetime import datetime
import pandas as pd
import cProfile
import io
import pstats
import contextlib
from deepctr.utils.logger import LoggerBuilder
from deepctr.database.sequel import Query

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.colheader_justify", "center")
pd.set_option("display.precision", 2)

# ------------------------------------------------------------------------------------------------ #
#                                    QUERY DECORATOR                                               #
# ------------------------------------------------------------------------------------------------ #
ERROR_LOGFILE = "logs/error.log"
DEBUG_LOGFILE = "logs/debug.log"


def query(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):

        logger_builder = LoggerBuilder()

        error_log = (
            logger_builder.reset()
            .set_level(level="error")
            .set_logfile(ERROR_LOGFILE)
            .build(name=func.__module__)
            .logger
        )

        debug_log = (
            logger_builder.reset()
            .set_level(level="debug")
            .set_logfile(DEBUG_LOGFILE)
            .build(name=func.__module__)
            .logger
        )

        try:
            start = datetime.now()
            result = func(self, *args, **kwargs)
            end = datetime.now()
            debug_log.debug(process_query(args, kwargs, start, end))
            return result

        except Exception as e:
            error_log.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper


def process_query(args, kwargs, start: datetime, end: datetime) -> str:
    """Extracts parameters from decorated function and creates the message.

    Args:
        args (tuple): Function arguments
        kwargs (dict): Function keyword arguments
        start (datetime): Datetime when wrapped function was called
        end (datetime): Datetime when wrapped function completed

    Returns (str): String message
    """
    params = None
    query = get_query(args, kwargs)
    if query.is_parameterized:
        params = get_params(args, kwargs)
    return format_msg(start, end, query, params)


def get_query(args, kwargs) -> Query:
    """Iterates through kwargs, searching for a Query object and returns the Query if found

    Args:
        args (tuple): Function arguments
        kwargs (dict): Function keyword arguments

    Returns:
        Query object

    """
    for arg in args:
        if isinstance(arg, Query):
            return arg
    for k, v in kwargs.items():
        if isinstance(v, Query):
            return v


def get_params(args, kwargs) -> Query:
    """Iterates through kwargs, searching for a tuple object containing function parameters.

    Args:
        args (tuple): Function arguments
        kwargs (dict): Function keyword arguments

    Returns:
        tuple object

    """
    for arg in args:
        if isinstance(arg, tuple):
            return arg
    for k, v in kwargs.items():
        if isinstance(v, tuple):
            return v


def format_msg(start: datetime, end: datetime, query: Query, params: tuple = None) -> str:
    duration = round((end - start).total_seconds(), 2)
    date = start.strftime("%m/%d/%y")
    time = start.strftime("%I:%M%p")
    if params is None:
        msg = "Query: {}\tStarted {} at {}\tDuration: {} seconds".format(
            query.name, date, time, duration
        )
    else:
        msg = "Query: {}. Params: {}\tStarted {} at {}\tDuration: {} seconds".format(
            query.name, params, date, time, duration
        )
    return msg


# ------------------------------------------------------------------------------------------------ #
#                                   OPERATOR DECORATOR                                             #
# ------------------------------------------------------------------------------------------------ #
def operator(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):

        logger_builder = LoggerBuilder()

        logger = (
            logger_builder.reset()
            .set_level(level="error")
            .set_logfile(ERROR_LOGFILE)
            .build(name=func.__module__)
            .logger
        )

        try:
            module = func.__module__
            classname = func.__qualname__
            start = datetime.now()
            print_start(module, classname, self, start)

            result = func(self, *args, **kwargs)
            end = datetime.now()

            print_end(module, classname, self, start, end)
            return result

        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper


def print_start(module: str, classname: str, self: str, start: datetime):
    task_no = self.__dict__["_task_no"]
    task_name = self.__dict__["_task_name"]
    module = module.split(".")[2]
    date = start.strftime("%d/%m/%y")
    time = start.strftime("%I:%M %p")
    msg = "Module: {}\t\tTask {}:\t{}\tStarted {} at {}.".format(
        str(module), str(task_no), task_name, date, time
    )
    print(msg)


def print_end(module: str, classname: str, self: str, start: datetime, end: datetime):
    task_no = self.__dict__["_task_no"]
    task_name = self.__dict__["_task_name"]
    duration = end - start
    duration = duration.total_seconds()
    module = module.split(".")[2]
    msg = "Module: {}\t\tTask {}:\t{}\tComplete.\tDuration: {} seconds.".format(
        str(module), str(task_no), task_name, str(round(duration, 2))
    )
    print(msg)


# ------------------------------------------------------------------------------------------------ #
#                                  PROFILE DECORATOR                                               #
# ------------------------------------------------------------------------------------------------ #


@contextlib.contextmanager
def profiled():
    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats()
    # uncomment this to see who's calling what
    ps.print_callers()
    print(s.getvalue())
