#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /reinstall.sh                                                                         #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, April 15th 2022, 3:02:54 pm                                                   #
# Modified : Tuesday, April 19th 2022, 2:10:28 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
echo $'\nStop MySQL processes..'
sudo /etc/init.d/mysql stop

echo $'\nRemoving existing installation..'
sudo apt purge mysql-server mysql-client mysql-common mysql-server-core-* mysql-client-core-*
sudo apt-get remove -y mysql-*
sudo apt-get purge -y mysql-*
sudo apt remove dbconfig-mysql

echo $'\nDelete all MySQL files...'
sudo rm -rf /etc/mysql /var/lib/mysql /var/log/mysql

echo $'\nCleaning packages not needed...'
#sudo apt-get autoclean not sure about this command
sudo apt autoremove
sudo apt autoclean
# Follow instructions at https://docs.microsoft.com/en-us/windows/wsl/tutorials/wsl-database
# use wsl terminal
echo $'\nUpdating distribution...'
sudo apt-get dist-upgrade

echo $'\nUpdating packages...'
sudo apt update

echo $'\nInstalling MySQL Server...'
sudo apt-get install mysql-server

echo $'\nStarting MySQL Server...'
sudo /etc/init.d/mysql start fails, try:

echo $'\nRunning secure installation...'
sudo mysql_secure_installation

echo $'\nOpen MySQL Prompt...'
sudo mysql -u root