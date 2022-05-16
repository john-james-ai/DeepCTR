#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deepctr: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /logger.py                                                                            #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, March 13th 2022, 1:41:40 pm                                                   #
# Modified : Thursday, April 21st 2022, 4:24:06 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
from abc import ABC
import os
import logging
from logging.handlers import TimedRotatingFileHandler

# ------------------------------------------------------------------------------------------------ #


class LoggerBuilder:
    """Builds a logger to specification"""

    __level = {
        "i": logging.INFO,
        "d": logging.DEBUG,
        "w": logging.WARN,
        "e": logging.ERROR,
    }

    def __init__(self) -> None:
        self.reset()

    def reset(self):
        self._logger = None
        self._console_log_format = "%(name)-12s: %(levelname)-8s %(message)s"
        return self

    @property
    def logger(self) -> logging.getLogger:
        logger = self._logger
        self.reset()
        return logger

    def set_logger(self, name: str) -> None:
        self.reset()
        self._logger = logging.getLogger(name)
        return self

    def set_level(self, level: str = "info") -> None:
        level = LoggerBuilder.__level.get(level[0], logging.INFO)
        self._logger.setLevel(level)
        return self

    def set_logfile(self, logfile: str = "logs/deepctr.log") -> None:
        self._logfile = logfile
        return self

    def set_console_log_format(self, log_format="%(name)-12s: %(levelname)-8s %(message)s"):
        self._console_log_format = log_format

    def set_console_handler(self):
        """Formats the handler and sets logger"""
        # Obtain handller
        handler = logging.StreamHandler()
        # Configure formatter
        format = logging.Formatter(self._console_log_format)
        # Set format on handler
        handler.setFormatter(format)
        # Set the handler on the logger
        self._logger.addHandler(handler)
        return self

    def set_file_handler(self, logfile: str):
        """Formats the operations file handler and sets logger"""
        os.makedirs(os.path.dirname(logfile), exist_ok=True)
        # Time rotating file handler is preferred
        handler = TimedRotatingFileHandler(filename=logfile)
        # Configure formatter for files to include time
        format = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
        # Set format on handler
        handler.setFormatter(format)
        # Set the handler on the logger
        self._logger.addHandler(handler)
        return self


class LogFactory(ABC):

    __LOGGER = None

    @staticmethod
    def __create_logger(logname: str, level: str = "info", **kwargs) -> logging.getLogger:
        logger_builder = LoggerBuilder()

        LogFactory.__LOGGER = (
            logger_builder.reset()
            .set_logger(logname)
            .set_level(level=level)
            .set_console_handler()
            .set_file_handler(kwargs.get("logfile", "deepctr.log"))
            .logger
        )
        return LogFactory.__LOGGER

    @staticmethod
    def get_logger(logname: str, level: str = "info", **kwargs) -> logging.getLogger:
        """
        A static method called by other modules to initialize logger in
        their own module
        """
        logger = LogFactory.__create_logger(logname, level, kwargs)

        # return the logger object
        return logger
