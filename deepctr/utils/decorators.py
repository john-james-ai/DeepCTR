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
from deepctr.utils.logger import LoggerBuilder

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
    date = start.strftime("%m/%d/%y")
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
