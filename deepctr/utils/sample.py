#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /sample.py                                                                            #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, April 24th 2022, 5:05:35 pm                                                   #
# Modified : Sunday, April 24th 2022, 5:19:14 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import pandas as pd
import random

# ------------------------------------------------------------------------------------------------ #


def sample_from_file(
    source: str, size: int, header: bool = True, random_state: int = 50
) -> pd.DataFrame:
    """Reads a random sampling of 'size' from 'source' file

    Args:
        source (str): The filepath to the file to be sampled
        size (int): Sample size
        header (bool): True if file contains a header row.
        random_state (int): Pseudo random seed
    Returns:
        pd.DataFrame
    """
    header_row = 1 if header else 0
    n = sum(1 for line in open(source)) - header_row  # Num lines in file minus header if exists
    skip = sorted(random.sample(range(header_row, n + 1), n - size))
    return pd.read_csv(source, skiprows=skip)
