#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /database.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 26th 2022 09:40:21 pm                                                  #
# Modified   : Thursday May 26th 2022 09:49:58 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""SQL related functions."""


def parse_sql(filename):
    data = open(filename, "r").readlines()
    stmts = []
    NEWLINE = "\n"
    DELIMITER = ";"
    stmt = ""

    for lineno, line in enumerate(data):
        if not line.strip():
            continue

        if line.startswith("/*") or line.startswith(" *"):
            continue

        if NEWLINE in line:
            line = line.replace(NEWLINE, "")

        if "DELIMITER" in line:
            DELIMITER = line.split()[1]
            continue

        if DELIMITER not in line:
            stmt += line.replace(DELIMITER, ";")
            continue

        if stmt:
            stmt += line
            stmts.append(stmt.strip())
            stmt = ""
        else:
            stmts.append(line.strip())
    return stmts
