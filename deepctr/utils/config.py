#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /config.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 14th 2022 02:41:42 am                                                  #
# Modified   : Saturday May 14th 2022 02:41:42 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import yaml
import yamlordereddictloader

# ------------------------------------------------------------------------------------------------ #


class YamlIO:
    """Reads and writes from and to Yaml files."""

    def read(self, filepath: str, **kwargs) -> dict:
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return yaml.load(f, Loader=yamlordereddictloader.Loader)
        else:
            return {}

    def write(self, data: dict, filepath: str, **kwargs) -> None:
        with open(filepath, "r") as f:
            yaml.dump(data, f)
