#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /build.sh                                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 21st 2022 03:10:32 am                                                  #
# Modified   : Friday June 17th 2022 11:45:11 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

echo $'\nRestarting MySQL - Pre-build...'
sudo /etc/init.d/mysql restart

echo $'\nBuild DeepCTR Dataset Database'
sudo mysql -u john -p < /home/john/projects/DeepCTR/deepctr/data/database.sql

echo $'\nRestarting MySQL - Post-build...'
sudo /etc/init.d/mysql restart
