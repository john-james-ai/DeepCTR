
#!/bin/bash 
# Download and install Spark 
# To suppress output add &> /dev/null to end of command line. 
# Removing /dev/null to ensure that everything is installed.

echo 'sudo apt-get update'
sudo apt-get update 
 
echo 'sudo apt install python3-pip -y'
sudo apt install python3-pip -y 
 
echo 'pip3 install jupyter'
pip3 install jupyter 
 
echo 'sudo apt-get install default-jre -y'
sudo apt-get install default-jre -y 
 
echo 'sudo apt-get install scala -y'
sudo apt-get install scala -y 
 
echo 'pip3 install py4j'
pip3 install py4j 
 
echo 'wget https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz'
wget https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz 
 
echo 'sudo tar -zxvf spark-3.2.1-bin-hadoop3.2.tgz'
sudo tar -zxvf spark-3.2.1-bin-hadoop3.2.tgz 
 
echo 'spark-3.2.1-bin-hadoop3.2.tgz'
sudo rm -r spark-3.2.1-bin-hadoop3.2.tgz 
 
cd ~
 
echo 'pip3 install findspark'
pip3 install findspark 
 
cd ~
 
echo 'export PATH=$PATH:~/.local/bin'
export PATH=$PATH:~/.local/bin
 
echo 'jupyter notebook --generate-config'
jupyter notebook --generate-config 
 
cd ~
 
echo "sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout j2spark.pem -out j2spark.pem -subj '/C=US/ST=California'"
mkdir certs
cd certs
touch ~/.rnd
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout j2spark.pem -out j2spark.pem -subj '/C=US/ST=California' 
sudo chmod 777 j2spark.pem
 
cd ~
 
echo 'jupyter echos'
echo "c = get_config()" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.certfile = u'/home/ubuntu/certs/j2spark.pem'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip = '0.0.0.0'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.allow_origin = '*'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = 8888" >> ~/.jupyter/jupyter_notebook_config.py
cd ~
jupyter notebook