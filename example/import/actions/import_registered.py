from dashgourd.importer.mysql import MysqlImportHelper

importer = MysqlImportHelper()
query = """
    select id _id, created_at from user
    """
importer.import_actions('registered', query)
importer.close()
