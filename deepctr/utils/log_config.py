#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /log_config.ini                                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday May 16th 2022 03:00:48 pm                                                    #
# Modified   : Monday May 16th 2022 03:00:48 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s | %(asctime)s | %(module)s | %(process)d  | %(thread)d  | %(message)s'
        },
        'standard': {
            'format': '%(levelname)s | %(asctime)s | %(message)s'
        },
        'simple': {
            'format': '%(message)s'
        },
    },
    'handlers': {
        'console':{
            'level':'DEBUG',
            'class':'logging.StreamHandler',
            'formatter': 'standard'
        },
        'file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'verbose',
            'filename': 'logs/deepctr.log',
            'backupCount': 3
        }
    },
    'loggers': {
        '': {
            'handlers':['console', 'file'],
            'propagate': False,
            'level':'INFO',
        }
    }
}