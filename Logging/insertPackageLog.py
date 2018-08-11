class PackageLog():
    # database connection layers
    import psycopg2
    import pymysql

    # datetime information
    import datetime
    import time

    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    # def insertSSISPackageLogDefaultValues(self):
    #     rowId  = self.insertSSISPackageLog(myPackageName = "test", myUsername = "bazam", mytxtMessage = "test")
    #     return rowId

    def insertSSISPackageLog(self, myPackageName = "Test", myVersionMajor = 1, myVersionMinor = 1, myVersionBuild = 1, myUsername ="test", myLastExecutionTime = "" , mytxtMessage = "", myStartTime = "", myEndTime = "", isSuccessful = 0):
        # mysql connection for config values
        mysqlconn = self.pymysql.connect()
        logCursor = mysqlconn.cursor()

        # parameters
        PackageName = myPackageName
        VersionMajor = myVersionMajor
        VersionMinor = myVersionMinor
        VersionBuild = myVersionBuild
        UserName = myUsername
        LastExecutionTime = myLastExecutionTime
        Debug = 1
        txtMessage = mytxtMessage
        StartTime =myStartTime
        EndTime = myEndTime
        Successful = isSuccessful


        logCursor.execute("""INSERT INTO 
                                SSISPackageLog 
                                (PackageName, VersionMajor, VersionMinor, VersionBuild, UserName, LastExecutionTime, Debug, txtMessage, StartTime, EndTime, Successful) 
                                VALUES
                                (%(PackageName)s, %(VersionMajor)s, %(VersionMinor)s, %(VersionBuild)s,%(UserName)s, %(LastExecutionTime)s, %(Debug)s, %(txtMessage)s,%(StartTime)s, %(EndTime)s, %(Successful)s)""", 
                                {
                                    'PackageName': PackageName, 
                                    'VersionMajor': VersionMajor, 
                                    'VersionMinor': VersionMinor, 
                                    'VersionBuild': VersionBuild, 
                                    'UserName': UserName, 
                                    'LastExecutionTime': LastExecutionTime, 
                                    'Debug': Debug, 
                                    'txtMessage': txtMessage, 
                                    'StartTime': StartTime, 
                                    'EndTime': EndTime, 
                                    'Successful': Successful
                                })


        rowId  = logCursor.lastrowid

        mysqlconn.commit()
        mysqlconn.close()
        return rowId



