#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /file.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 20th 2022 05:16:06 am                                                    #
# Modified   : Friday May 20th 2022 05:16:07 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Temporary File and Directory classes that accept directory and filenames"""
import shutil

# ------------------------------------------------------------------------------------------------ #


class TempFile:
    def __init__(self, filepath: str, data=True, format="csv") -> None:
        self._filepath = filepath
        self._data = data
        self._format = format

    def __enter__(self):
        return self._filepath

    def __exit__(self, exception_type=None, exception_value=None, traceback=None):
        shutil.rmtree(self._filepath, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ #


class TempDirectory:
    def __init__(self, directory: str, data=True) -> None:
        self._directory = directory
        self._data = data

    def __enter__(self):
        return self._directory

    def __exit__(self, exception_type=None, exception_value=None, traceback=None):
        shutil.rmtree(self._directory, ignore_errors=True)
