from dashgourd.importer.mysql import MysqlImportHelper

importer = MysqlImportHelper()
query = """
    select u.id _id, u.referrer, u.birthday, u.gender, u.created_at from user'
    """
importer.import_users(query)
importer.close()
