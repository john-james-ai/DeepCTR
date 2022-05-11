#!/usr/bin/bash
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /setup.sh                                                                             #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Thursday, April 21st 2022, 9:02:53 am                                                 #
# Modified : Thursday, April 21st 2022, 11:11:36 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
echo $'\nStop MySQL processes..'
sudo /etc/init.d/mysql stop

echo $'\nRemoving existing installation..'
sudo apt purge mysql-server mysql-client mysql-common mysql-server-core-* mysql-client-core-* -y
sudo apt-get remove mysql-* -y
sudo apt-get purge mysql-* -y
sudo apt remove dbconfig-mysql -y

echo $'\nDelete all MySQL files...'
sudo rm -rf /etc/mysql /var/lib/mysql /var/log/mysql

echo $'\nCleaning packages not needed...'
sudo apt-get autoclean -y
sudo apt autoremove -y
sudo apt autoclean -y
# Follow instructions at https://docs.microsoft.com/en-us/windows/wsl/tutorials/wsl-database
# use wsl terminal
echo $'\nUpdating distribution...'
sudo apt-get dist-upgrade -y

echo $'\nUpdating packages...'
sudo apt update -y

echo $'\nInstalling MySQL Server...'
sudo apt-get install mysql-server -y

echo $'\nStarting MySQL Server...'
sudo /etc/init.d/mysql start

echo $'\nRunning secure installation...'
sudo mysql_secure_installation

echo $'\nStart Database...'
sudo /etc/init.d/mysql restart

echo $'\nSetup User and Databases...'
sudo mysql -u root -p --database mysql < deepctr/database/setup.sql

echo $'\nSign in as new user...'
sudo mysql -u john -p
