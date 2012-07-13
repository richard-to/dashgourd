import MySQLdb

class MysqlImport(object):
    """Imports data from MySQL to DashGourd.
    
    The importer can import users and events.
    Users cannot be imported with events yet.
    Events can be imported if the user exists.
    Also make sure field names are labeled correctly.
    Users and Events require an _id attribute.
    
    Attributes:
        mysql_conn: MySQLdb connection
        api: DashGourd api object
    """
     
    def __init__(self, mysql_conn, api):
        self.mysql_conn = mysql_conn
        self.api = api
    
    
    """Imports users into DashGourd.
    
    The data will be inserted as is into the user collection.
    This method inserts new users and does not update them.
    
    Make sure one field is named _id.
    
    "events" is reserved for user events
    
    Note that users are not inserted in batch. 
    They are inserted one at a time.
    
    Attributes:
        query: MySQL query to run
    """
        
    def import_users(self, query):
        cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query)
        numrows = int(cursor.rowcount)
        
        for i in range(numrows):
            data = cursor.fetchone()
            if data['_id'] is not None and data['events'] is None:
                self.api.create_user(data)
                        
        cursor.close()        
    
    def import_events(self, query):
       print events 


    
    def close(self):