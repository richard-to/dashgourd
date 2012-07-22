import os
import MySQLdb
from dashgourd.api.actions import ActionsApi
from dashgourd.api.helper import get_mongodb_db
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
             
    def import_users(self, query):
        """Imports users into DashGourd.
        
        The data will be inserted as is into the user collection.
        This method inserts new users and does not update them.
        
        Make sure one field is named `user_id`.
        
        `actions` is reserved for user actions
        
        Note that users are not inserted in batch. 
        They are inserted one at a time.
        
        Args:
            query: MySQL query to run
        """
            
        if self.mysql_conn.open:        
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query)
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):
                data = cursor.fetchone()
                self.api.create_user(data)
                            
            cursor.close()        
        
    def import_actions(self, name, query):
        """Imports actions into DashGourd
        
        The data will be inserted into the embedded document list named
        `actions`.
        
        The data must include the following fields `user_id`, `name`, `created_at`.
        If the data does not contain those fields, then the api will fail silently
        and not insert that row.
        
        Args:
            name: Action name
            query: MySQL query to run
        """
                
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

    def import_abtests(self, abtest, query):
        """Imports abtests into DashGourd
        
        The data will be inserted into an object named `ab`
        
        The data must include the following fields `user_id`, `abtest`, `variation`.
        
        If the data does not contain those fields, then the api will fail silently
        and not insert that row.
        
        Args:
            abtest: Ab test name
            query: MySQL query to run
        """        
        if self.mysql_conn.open:
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query)
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):        
                data = cursor.fetchone()
                data['abtest'] = abtest
                self.api.tag_abtest(data)
            
            cursor.close() 
             
    def close(self):
        """Closes MySQL connection
        
        When the connection is closed, the import methods
        will fail silently for now.
        """
        
        self.mysql_conn.close()

def get_mysql_conn():
    """Helper to get mysql connection
    
    Depends on various os environment variables to 
    be set.
    
    Returns:
        MySQLdb connection
        
    """
    
    return MySQLdb.connect(
            user=os.environ.get('MYSQL_USER'),
            passwd= os.environ.get('MYSQL_PASS'),
            db= os.environ.get('MYSQL_DB'),
            host= os.environ.get('MYSQL_HOST', 'localhost'),
            port= os.environ.get('MYSQL_PORT', 3307)) 
    
class MysqlImportHelper(object):
    """Boilerplate wrapper for MysqlImporter
     
    Provides boiler plate db initialization via 
    environment variables.
     
    Just need to provide the query with the helper. Not too
    flexible, but I don't need much more for importing.            
    """
    
    def __init__(self):

        mongo_db = get_mongodb_db()            
        conn = get_mysql_conn()        
        
        api = ActionsApi(mongo_db)
        self.importer = MysqlImporter(conn, api)
        
    def import_users(self, query):
        """Wrapper for MysqlImporter.import_users
        
        Args:
            query: Query used to import users
        """
            
        self.importer.import_users(query)
            
    def import_actions(self, name, query):
        """Wrapper for MysqlImporter.import_actions
    
        Args:
            name: Action name
            query: Query used to import actions
        """
        
        self.importer.import_actions(name, query) 
   
    def import_abtests(self, abtest, query):
        """Wrapper for MysqlImporter.import_actions
    
        Args:
            abtest: Abtest name
            query: Query used to import actions
        """
        
        self.importer.import_abtests(abtest, query) 
                   
    def close(self):
        self.importer.close()