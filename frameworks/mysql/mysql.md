# See if any mysql processes are running
sudo  ps -ef | grep mysql
# Kill the process if it is running
sudo kill -9 'pid' mysqld
# Run MySQL safe daemon with skipping grant tables
sudo mysqld_safe --skip-grant-tables &
sudo mysqld --skip-grant-tables
# Login to MySQL as root with no password
sudo mysql -u root mysql
# Setting the root password and building the airflow metadata database
sudo mysql -u root
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'the_secret_password';
FLUSH PRIVILEGES;
exit from mysql
sudo /etc/init.d/mysql restart

# Uninstall and reinstall
1. First, remove already installed mysql-server using--
sudo apt-get remove --purge mysql-server mysql-client mysql-common
2. Then clean all files
sudo apt-get autoremove
sudo apt-get remove -y mysql-*
sudo apt-get purge -y mysql-*
3. Update Packages
sudo apt update
4. Install MySQL
sudo apt install mysql-server
5. Start MySQL Server
sudo /etc/init.d/mysql start
6. Run Security Installation
sudo mysql_secure_installation
7. Open MySQL Prompt
sudo mysql -u root -p


# Start and login
sudo /etc/init.d/mysql start

# Kill Process
The command to show running mysql proesses is:
ps aux | grep mysql
To kill the process, run
sudo kill -9 process_id