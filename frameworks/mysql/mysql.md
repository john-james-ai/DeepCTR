# Trouble Shotting

## Processes
To see which processes are running:
    sudo  ps -ef | grep mysql
    sudo ps aux | grep mysqld
    sudo ps aux | grep mysql

Killing Processes: Single
    sudo kill -9 'pid'

Killing Processes: All
    killall -9 mysql
    killall -9 mysqld
    killall -9 mysqld_safe

## Grant Tables
Run MySQL safe daemon with skipping grant tables
    sudo mysqld_safe --skip-grant-tables &
    sudo mysqld --skip-grant-tables
## Login to MySQL
Root w/o password
    sudo mysql -u root mysql
Setting the root password and building the airflow metadata database
    sudo mysql -u root
    ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'the_secret_password';
    FLUSH PRIVILEGES;
    exit from mysql
    sudo /etc/init.d/mysql restart


# Start and login
sudo /etc/init.d/mysql start
