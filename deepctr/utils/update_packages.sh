#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepNeuralCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /update_packages.sh                                                                   #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepNeuralCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, April 10th 2022, 3:55:33 pm                                                   #
# Modified :                                                                                       #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #

<<comment
the apt-get dist-upgrade also upgrades the packages. In addition to this, it also handles changing dependencies with the latest versions of the package. It intelligently resolves the conflict among package dependencies and tries to upgrade the most significant packages at the expense of less significant ones, if required. Unlike apt-get upgrade command, the apt-get dist-upgrade is proactive and it installs new packages or removes existing ones on its own in order to complete the upgrade
comment
sudo apt-get dist-upgrade