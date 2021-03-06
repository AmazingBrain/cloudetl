# accept command line args
import argparse

# database connection layers
import psycopg2
import pymysql

# datetime information
import datetime

# aws connection
import boto3

# log start
print("Starting Process: " + str(datetime.datetime.now()))

datetimestring = str(datetime.datetime.now()).replace("-","").replace(":","").replace(".","").replace(" ","")[:-6]

# Get ETL ProcessName
# processname = input("Enter ETL Master Process Name: ")
parser = argparse.ArgumentParser(description='Process Master ETL Process Name')
parser.add_argument('etlname', help="Enter the Master Process ETL Name")
args = parser.parse_args()

processname = args.etlname

# mysql connection for config values
mysqlconn = pymysql.connect()
configcursor = mysqlconn.cursor()

configcursor.execute(
                        """SELECT   cp.ChildProcessName,
                                    ds.DataSourceId,
                                    ds.DataSourceTypeId,
                                    dd.DataSourceId as DataDestinationId,
                                    dd.DataSourceTypeId as DataDestinationTypeId,
                                    ds.ConnectionString as DataSourceConnectionString,
                                    dd.ConnectionString as DataDestinationConnectionString,
                                    cp.ETLCode,
                                    cp.ETLCodeTypeId
                            FROM      MasterProcess mp
                            INNER JOIN  ChildProcess cp on mp.MasterProcessId = cp.MasterProcessId
                            INNER JOIN  DataSource ds ON cp.DataSourceId = ds.DataSourceId
                            INNER JOIN  DataSource dd ON cp.DataDestinationId = dd.DataSourceId

                            WHERE     mp.ProcessName = '"""+processname+"""'
                            AND ChildProcessTypeId = 2
                            ORDER BY Priority, Sequence;"""
                    )

configvalues = configcursor.fetchall()

mysqlconn.close()

# assign values, copy data into Redshift, and archive files
for row in configvalues:
    childprocessname = row[0]
    datasourceid = row[1]
    datasourcetypeid = row[2]
    datadestinationid = row[3]
    datadestinationtypeid = row[4]
    datasourceconnectionstring = row[5]
    datadestinationconnectionstring = row[6]
    etlcode = row[7]
    etlcodetypeid = row[8]

    importfilesdir = "PROD/ImportFiles/"
    archivefilesdir = "PROD/ImportFiles/Archive/"

    session = boto3.Session(aws_access_key_id='',aws_secret_access_key='',region_name='')
    s3 = session.resource('s3')
    bucket = s3.Bucket(datasourceconnectionstring)

# loop through bucket to find files. Need to replace if statements with object.filter()
    for files in bucket.objects.all():
        if importfilesdir+childprocessname in files.key:
            importfile = files.key
            etlcodeexec = ""

            if (etlcodetypeid == 1):
                etlcodeexec = etlcode.replace("?",importfile)
            elif (etlcodetypeid == 3):
                for codefiles in bucket.objects.all():
                    if etlcode in codefiles.key:
            
                        etlcodeexec = codefiles.get()['Body'].read()
                        etlcodeexec = etlcodeexec.decode().rstrip().replace("?",importfile)
            
                        
            # connect to redshift, run copy commands
            redshiftconn = psycopg2.connect(datadestinationconnectionstring)
            importcursor = redshiftconn.cursor()

            # substitue the import file for "?" in the etlcode
            # etlcodeexec = etlcodeexec.replace("?",importfile)

            importcursor.execute(etlcodeexec)
            redshiftconn.commit()
            redshiftconn.close()

            # archive import file
            copysource = {'Bucket': datasourceconnectionstring, 'Key': importfile }
            s3.meta.client.copy(copysource, datasourceconnectionstring, archivefilesdir+importfile[importfile.rfind("/")+1:len(importfile)])

            # delete import file
            s3.Object(datasourceconnectionstring,importfile).delete()

# log completion time
print("Process Completed: " + str(datetime.datetime.now()))
            


        
