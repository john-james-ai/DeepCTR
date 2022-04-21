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
        self._level = (logging.INFO,)
        self._logfile = "logs/deepctr.log"
        return self

    @property
    def logger(self) -> logging.getLogger:
        logger = self._logger
        self.reset()
        return logger

    def set_level(self, level: str = "info") -> None:
        self._level = LoggerBuilder.__level.get(level[0], logging.INFO)
        return self

    def set_logfile(self, logfile: str = "logs/deepctr.log") -> None:
        self._logfile = logfile
        return self

    def build_console_handler(self):
        """Formats the handler and sets logger"""
        # Obtain handller
        handler = logging.StreamHandler()
        # Configure formatter
        format = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
        # Set format on handler
        handler.setFormatter(format)
        return handler

    def build_file_handler(self, logfile: str):
        """Formats the operations file handler and sets logger"""
        os.makedirs(os.path.dirname(logfile), exist_ok=True)
        # Time rotating file handler is preferred
        handler = TimedRotatingFileHandler(filename=self._logfile)
        # Configure formatter for files to include time
        format = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
        # Set format on handler
        handler.setFormatter(format)
        return handler

    def build(self, name: str):
        # Instantiate logger with 'info' level. User can override
        self._logger = logging.getLogger(name)
        self._logger.setLevel(self._level)
        self._logger.addHandler(self.build_console_handler())
        self._logger.addHandler(self.build_file_handler(self._logfile))
        return self
