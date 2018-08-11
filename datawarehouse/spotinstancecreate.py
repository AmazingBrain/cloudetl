import boto3
import datetime
import base64
import uuid
# accept command line args
import argparse

# Get ETL ProcessName
# processname = input("Enter ETL Master Process Name: ")
parser = argparse.ArgumentParser(description='Process Master ETL Process Name')
parser.add_argument('etlname', help="Enter the Master Process ETL Name")
args = parser.parse_args()

processname = args.etlname

# processname = base64.b64encode(processname.encode()).decode("ascii")

clienttoken = str(uuid.uuid4())

client = boto3.client('ec2',aws_access_key_id='',
        aws_secret_access_key='',
        region_name='')

userdata = """#!/bin/bash
sudo mkfs -t ext4 /dev/nvme1n1
sudo mkdir /data
sudo mount /dev/nvme1n1 /data
sudo mkdir -p /data/storage/etl
sudo git clone git@bitbucket.org:renovateamerica/etl.git /data/source/etl
sudo mkdir -p /data/source/etl/datawarehouse/export/PROD/ImportFiles
sudo python3 /data/source/etl/datawarehouse/s3export.py """+processname+"""
sudo python3 /data/source/etl/datawarehouse/s3import.py """+processname+"""
sudo python3 /data/source/etl/datawarehouse/dwloadpublic.py """+processname+"""
sudo aws ec2 terminate-instances --instance-ids $(curl -s http://169.254.169.254/latest/meta-data/instance-id) --region us-west-2"""

userdataencode = base64.b64encode(userdata.encode()).decode("ascii")

response = client.request_spot_instances(
    DryRun=False,
    SpotPrice='0.15',
    ClientToken=clienttoken,
    InstanceCount=1,
    Type='one-time',
    LaunchSpecification={
        "ImageId": "ami-13d8fd6b",
      "InstanceType": "m5d.2xlarge",
      "SubnetId": "subnet-ce1019ac",
      "KeyName": "AWSBI",
      "BlockDeviceMappings": [
        {
          "DeviceName": "/dev/xvda",
          "Ebs": {
            "DeleteOnTermination": True,
            "VolumeType": "gp2",
            "VolumeSize": 20,
            "SnapshotId": "snap-039eed3d7c83008f7"
          }
        }
      ],
      "SecurityGroupIds": [
        "sg-bf49facf",
      ],
      "UserData": userdataencode
    }
)

print(response)