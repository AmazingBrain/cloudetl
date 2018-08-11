# multiprocessing
from multiprocessing import Pool

# accept command line args
import argparse

# database connection layers
import pyodbc
import pymysql

# datetime information
import datetime

# export data
import csv
import os
import hashlib
import gzip
import shutil

# aws connection
import boto3

def childprocess(configvalues):
    childprocessname = configvalues[0]
    datasourceid = configvalues[1]
    datasourcetypeid = configvalues[2]
    datadestinationid = configvalues[3]
    datadestinationtypeid = configvalues[4]
    datasourceconnectionstring = configvalues[5]
    datadestinationconnectionstring = configvalues[6]
    etlcode = configvalues[7]
    etlcodetypeid = configvalues[8]
    priority = configvalues[9]
    sequence = configvalues[10]

    print("Starting Processing for "+childprocessname+": " + str(datetime.datetime.now()))

    
    # if the datasource is SQL Server and the ETL Code Type is Raw SQL in the config table
    if (datasourcetypeid == 1):
        # setup sql server connection for source queries
        mssqlconn = pyodbc.connect(datasourceconnectionstring)
        sourcecursor = mssqlconn.cursor()

        # execute etlcode based on type
        if(etlcodetypeid == 1):
            sourcecursor.execute(etlcode)
        elif (etlcodetypeid == 2):
            sourcecursor.execute("EXEC "+etlcode)


        # get results from query
        result = sourcecursor.fetchall()

        # close source database connection
        mssqlconn.close()

        # convert results to list for more flexible operations downstream
        rowlist = [list(i) for i in result]

        # dynamically grab all headers from the source query
        headerstuple = ([i[0] for i in sourcecursor.description])

        # load headers tuple into a list, add the HashValue column, then convert the list back to a tuple
        headerlist = list(headerstuple)
        headerlist.append("HashValue")
        headerstuple = tuple(headerlist)

        # declaration of resultslist to hold query results plus a calculated HashValue
        resultslist = []

        # nested loop to concatenate all columns in a row, convert them to unicode values for hashing, generate the hash, then add the results to the list above
        for items in rowlist:
            i = 0
            rowcode = str("").encode('utf-8')
            for item in items:
                rowcode += str(item).encode('utf-8')
            rowhash = hashlib.sha256(rowcode).hexdigest()
            items.append(rowhash)
            resultslist.insert(i,items)
            i += 1

        # setting up environment variables. these will come from a database later
        environment = "PROD"
        exportdirectory = "export/"+environment+"/ImportFiles/"

        # create the export directory tree if it doesn't exist
        os.makedirs(exportdirectory, exist_ok=True)

        # name the output file
        outputfile = childprocessname+"_"+datetimestring+".pipe"

        # create the output destination string and zip string
        outputdest = exportdirectory+outputfile
        outputdestzip = outputdest+".gz"

        # write the results with headers to a pipe delimited file
        with open(outputdest,"w") as outfile:
            writer = csv.writer(outfile,delimiter="|",lineterminator='\n')
            writer.writerow(headerstuple)
            writer.writerows(resultslist)

        # compress the file for uploading to s3 and faster import to Redshift
        with open(outputdest, 'rb') as zipin:
            with gzip.open(outputdestzip, 'wb', 9) as zipout:
                shutil.copyfileobj(zipin, zipout)

        # remove uncompressed file
        os.remove(outputdest)

        # upload files using function we built above
        upload_files(outputdestzip,datadestinationconnectionstring)

        # remove the local compressed file
        os.remove(outputdestzip)
        print("Process Completed for "+childprocessname+": " + str(datetime.datetime.now()))

# function to upload files to s3
def upload_files(file,bucket):
    s3 = boto3.client('s3',aws_access_key_id='',
        aws_secret_access_key='',
        region_name='')

    filename = os.path.basename(file)
    s3.upload_file(file,bucket,'PROD/ImportFiles/{}'.format(filename))

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
                                    cp.ETLCodeTypeId,
                                    cp.Priority,
                                    cp.Sequence
                            FROM      MasterProcess mp
                            INNER JOIN  ChildProcess cp on mp.MasterProcessId = cp.MasterProcessId
                            INNER JOIN  DataSource ds ON cp.DataSourceId = ds.DataSourceId
                            INNER JOIN  DataSource dd ON cp.DataDestinationId = dd.DataSourceId

                            WHERE     mp.ProcessName = '"""+processname+"""'
                            AND ChildProcessTypeId = 1
                            ORDER BY Priority, Sequence;"""
                    )

configvalues = configcursor.fetchall()

mysqlconn.close()

if __name__ == '__main__':
    p = Pool(os.cpu_count() - 1)
    p.map(childprocess,configvalues)
    p.close()
    p.join()
