from dashgourd.importer.mysql import MysqlImportHelper

importer = MysqlImportHelper()
query = """
    select id _id, referrer, birthday, gender, created_at from user
    """
importer.import_users(query)
importer.close()
