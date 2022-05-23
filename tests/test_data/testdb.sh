#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /testdb.sh                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 21st 2022 03:10:32 am                                                  #
# Modified   : Monday May 23rd 2022 03:41:03 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

echo $'\nRestarting MySQL - Pre-build...'
sudo /etc/init.d/mysql restart

echo $'\nBuild Test Database'
sudo mysql -u john -p < tests/test_data/testdb.sql

echo $'\nRestarting MySQL - Post-build...'
sudo /etc/init.d/mysql restart
