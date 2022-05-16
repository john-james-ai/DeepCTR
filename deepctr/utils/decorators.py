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
from deepctr.utils.logger import ConsoleLogFactory, FileLogFactory
from deepctr.utils.printing import Printer

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.colheader_justify", "center")
pd.set_option("display.precision", 2)


# ------------------------------------------------------------------------------------------------ #
#                                   OPERATOR DECORATOR                                             #
# ------------------------------------------------------------------------------------------------ #
def operator(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):

        OPERATOR_LOGFILE = "logs/operators.log"

        # Creating separate loggers so that each logger only prints to a single output
        # to avoid each message printing twice to console.
        console_logname = func.__class__.__name__ + "_console"
        file_logname = func.__class__.__name__ + "_file"

        console_logger = ConsoleLogFactory().get_logger(console_logname, level="info")
        file_logger = FileLogFactory().get_logger(
            file_logname, level="info", logfile=OPERATOR_LOGFILE
        )
        loggers = {"console": console_logger, "file": file_logger}

        try:
            module = func.__module__
            classname = func.__qualname__
            start = datetime.now()
            print_start(module, classname, self, start, file_logger)

            result = func(self, *args, **kwargs)
            end = datetime.now()

            print_end(module, classname, self, start, end, file_logger)
            return result

        except Exception as e:
            for logger in loggers.values():
                logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper


def print_start(module: str, classname: str, self: str, start: datetime, logger):
    printer = Printer()
    printer.print_blank_line()

    print_line = {}

    task = "Task " + str(self.__dict__["_task_no"]) + ":"
    print_line[task] = 10

    task_name = self.__dict__["_task_name"]
    print_line[task_name] = 40

    time = start.strftime("%I:%M %p")
    dt = "Started at {}".format(time)
    print_line[dt] = 20

    print_string = printer.print_aligned(content=print_line, return_line=True)

    logger.info(print_string)


def print_end(module: str, classname: str, self: str, start: datetime, end: datetime, logger):
    printer = Printer()

    duration = end - start
    duration = duration.total_seconds()

    print_line = {}

    task = "Task " + str(self.__dict__["_task_no"]) + ":"
    print_line[task] = 10

    task_name = self.__dict__["_task_name"]
    print_line[task_name] = 50

    time = start.strftime("%I:%M %p")
    dt = "Completed at {} (Duration: {} seconds.)".format(time, str(round(duration, 2)))
    print_line[dt] = 0

    print_string = printer.print_aligned(content=print_line, return_line=True)

    logger.info(print_string)
