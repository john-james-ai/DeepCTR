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
# Modified : Wednesday, April 20th 2022, 1:24:04 am                                                #
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

echo $'\nUpdating and upgrading distribution'
sudo apt-get update && sudo apt-get dist-upgrade

echo $'\nInstalling MySQL Server...'
sudo apt-get install mysql-server

echo $'\nStarting MySQL Server...'
sudo /etc/init.d/mysql start

echo $'\nSet root user authentication'
sudo mysql -u root -p < frameworks/mysql/auth.sql

echo $'\nRunning secure installation...'
sudo mysql_secure_installation

echo $'\nSetting home directory...'
sudo /etc/init.d/mysql stop
sudo usermod -d /var/lib/mysql/ mysql
sudo /etc/init.d/mysql restart

echo $'\nOpen MySQL Prompt...'
sudo mysql -u root -p < frameworks/mysql/metadata_ddl.sql

echo $'\nRestarting MySQL..'
sudo /etc/init.d/mysql restart

echo $'\nOpen MySQL Prompt...'
sudo mysql -u root
