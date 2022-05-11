
#!/bin/bash
echo 'Change permissions on pem file'
chmod 400 /home/john/projects/DeepCTR/frameworks/aws/j2spark.pem

echo 'Connect to DNS' 
ssh -i frameworks/aws/j2spark.pem ubuntu@ec2-3-23-60-23.us-east-2.compute.amazonaws.com

echo ''