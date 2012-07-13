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
    
    Make sure one field is named `_id`.
    
    `_events` is reserved for user events
    
    Note that users are not inserted in batch. 
    They are inserted one at a time.
    
    Args:
        query: MySQL query to run
    """
        
    def import_users(self, query):
        
        if self.mysql_conn.open:        
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query)
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):
                data = cursor.fetchone()
                self.api.create_user(data)
                            
            cursor.close()        
    
    """Imports events into DashGourd
    
    The data will be inserted into the embedded document list named
    `_events`.
    
    The data must include the following fields `_id`, `_type`, `_created_at`.
    If the data does not contain those fields, then the api will fail silently
    and not insert that row.
    
    Args:
        query: MySQL query to run
    """
    
    def import_events(self, query):
        
        if self.mysql_conn.open:
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query)
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):        
                data = cursor.fetchone()
                self.api.insert_event(data)
            
            cursor.close() 

    """Closes MySQL connection
    
    When the connection is closed, the import methods
    will fail silently for now.
    """    
    def close(self):
        self.mysql_conn.close()
        