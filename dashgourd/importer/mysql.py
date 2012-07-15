import os
import MySQLdb
from dashgourd.api.actions import ActionsApi
from pymongo import Connection

class MysqlImporter(object):
    """Imports data from MySQL to DashGourd.
    
    The importer can import users and actions.
    Users cannot be imported with actions yet.
    Actions can be imported if the user exists.
    Also make sure field names are labeled correctly.
    Users and Actions require an _id attribute.
    
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
    
    `actions` is reserved for user actions
    
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
    
    """Imports actions into DashGourd
    
    The data will be inserted into the embedded document list named
    `actions`.
    
    The data must include the following fields `_id`, `name`, `created_at`.
    If the data does not contain those fields, then the api will fail silently
    and not insert that row.
    
    Args:
        name: Action name
        query: MySQL query to run
    """
    
    def import_actions(self, name, query):
        
        self.api.register_action(name)
                    
        if self.mysql_conn.open:
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query)
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):        
                data = cursor.fetchone()
                data['name'] = name
                self.api.insert_action(data)
            
            cursor.close() 

    """Closes MySQL connection
    
    When the connection is closed, the import methods
    will fail silently for now.
    """    
    def close(self):
        self.mysql_conn.close()
 
 
class MysqlImportHelper(object):
    """Boilerplate wrapper for MysqlImporter
     
    Provides boiler plate db initialization via 
    environment variables.
     
    Just need to provide the query with the helper. Not too
    flexible, but I don't need much more for importing.            
    """
    
    def __init__(self):
        connection = Connection(
            os.environ.get('MONGO_HOST', 'localhost'), 
            os.environ.get('MONGO_PORT', 27017))
        mongo_db = connection[os.environ.get('MONGO_DB')]
        
        mongo_user = os.environ.get('MONGO_USER')
        mongo_pass = os.environ.get('MONGO_PASS')
        
        if mongo_user is not None and mongo_pass is not None:
            mongo_db.authenticate()
            
        conn = MySQLdb.connect(
            user=os.environ.get('MYSQL_USER'),
            passwd= os.environ.get('MYSQL_PASS'),
            db= os.environ.get('MYSQL_DB'),    
            host= os.environ.get('MYSQL_HOST', 'localhost'),
            port= os.environ.get('MYSQL_PORT', 3307))        
        
        api = ActionsApi(mongo_db)
        self.importer = MysqlImporter(conn, api)
    
    """Wrapper for MysqlImporter.import_users
    
    Args:
        query: Query used to import users
    """
    
    def import_users(self, query):
        self.importer.import_users(query)

    """Wrapper for MysqlImporter.import_actions
    
    Args:
        name: Action name
        query: Query used to import actions
    """
            
    def import_actions(self, name, query):
        self.importer.import_actions( name, query) 
        
    """Wrapper for MysqlImporter.close
    """    
    def close(self):
        self.importer.close()               