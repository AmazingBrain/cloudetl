class TaskLog():
    # database connection layers
    import psycopg2
    import pymysql

    # datetime information
    import datetime
    import time

    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    # def insertSSISTaskLogDefaultValues(self):
    #     rowId  = self.insertSSISTaskLog(myPackageId = 1, myTaskName = "Test", myErrorMessage = "test")
    #     return rowId

    def insertSSISTaskLog(self, myPackageId = 1, myTaskName = "test", myInsertedRowCount = 1, myUpdatedRowCount = 1 , myDeletedRowCount = 1, myErrorRowCount=1, myErrorMessage="test", myStartTime="", myEndTime="", isSuccessful =0):
        # mysql connection for config values
        mysqlconn = self.pymysql.connect()
        logCursor = mysqlconn.cursor()

        # parameters
        PackageLogId = myPackageId
        TaskName = myTaskName
        InsertedRowCount = myInsertedRowCount
        UpdatedRowCount = myUpdatedRowCount
        DeletedRowCount = myDeletedRowCount
        ErrorRowCount = myErrorRowCount
        ErrorMessage = myErrorMessage
        StartTime =myStartTime
        EndTime = myEndTime
        Successful = isSuccessful


        logCursor.execute("""INSERT INTO 
                                SSISTaskLog 
                                (PackageLogId, TaskName, InsertedRowCount, UpdatedRowCount, DeletedRowCount, ErrorRowCount, ErrorMessage, StartTime, EndTime, Successful) 
                                VALUES
                                (%(PackageLogId)s, %(TaskName)s, %(InsertedRowCount)s, %(UpdatedRowCount)s,%(DeletedRowCount)s, %(ErrorRowCount)s, %(ErrorMessage)s, %(StartTime)s, %(EndTime)s, %(Successful)s)""", 
                                {
                                    'PackageLogId': PackageLogId, 
                                    'TaskName': TaskName, 
                                    'InsertedRowCount': InsertedRowCount, 
                                    'UpdatedRowCount': UpdatedRowCount, 
                                    'DeletedRowCount': DeletedRowCount, 
                                    'ErrorRowCount': ErrorRowCount, 
                                    'ErrorMessage': ErrorMessage, 
                                    'StartTime': StartTime, 
                                    'EndTime': EndTime, 
                                    'Successful': Successful
                                })


        rowId  = logCursor.lastrowid

        mysqlconn.commit()
        mysqlconn.close()
        return rowId