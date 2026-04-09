import sys
import pymysql
from pymysql.cursors import DictCursor as sqlDictCursor

#从Mysql到Hive数据类型的映射关系
# ... existing code ...
#从Mysql到Hive数据类型的映射关系
TYPE_MAPPING = {
    'int': 'INT',
    'integer': 'INT',
    'tinyint': 'TINYINT',
    'smallint': 'SMALLINT',
    'mediumint': 'INT',
    'bigint': 'BIGINT',
    'float': 'FLOAT',
    'double': 'DOUBLE',
    'decimal': 'DECIMAL',
    'numeric': 'DECIMAL',
    'char': 'STRING',
    'varchar': 'STRING',
    'text': 'STRING',
    'tinytext': 'STRING',
    'mediumtext': 'STRING',
    'longtext': 'STRING',
    'date': 'DATE',
    'datetime': 'TIMESTAMP',
    'timestamp': 'TIMESTAMP',
    'time': 'STRING',
    'year': 'INT',
    'blob': 'BINARY',
    'tinyblob': 'BINARY',
    'mediumblob': 'BINARY',
    'longblob': 'BINARY',
    'binary': 'BINARY',
    'varbinary': 'BINARY',
    'bit': 'BOOLEAN',
    'boolean': 'BOOLEAN',
    'enum': 'STRING',
    'set': 'STRING',
    'json': 'STRING'
}


#查询mysql元数据,获取数据库表结构
def getDBDate(dbName, tableName):
    #链接mysql数据库
    conn = pymysql.connect(
        host='node1',
        user='root',
        password='592251@Ks',
        db='mysql',
        port=3306,
        charset='utf8'
    )
    curses = conn.cursor(sqlDictCursor)
    sql="""
        select column_name, data_type
        from information_schema.`columns`
        where table_schema = '%s' and table_name = '%s'
        """
    curses.execute(sql % (dbName, tableName))
    result = curses.fetchall() #返回格式为[{'column_name': 'id', 'data_type': 'int'}, {'column_name': 'name', 'data_type': 'varchar'}]
    if not result:
        print('%s.%s表不存在' % (dbName, tableName))
        sys.exit(-1)

    #将字典key转换为小写
    result_lower = []
    for item in result:
        lower_item = {k.lower(): v for k, v in item.items()}
        result_lower.append(lower_item)
    # print(result_lower)
    curses.close()
    conn.close()
    return result_lower


def createTableSql(dbName, tableName, field_info):
    create_sql = f"CREATE TABLE IF NOT EXISTS ods_jrxd_{tableName} ("
    for i in field_info:
        field_name = i['column_name']
        field_type = i['data_type']
        hive_type = TYPE_MAPPING.get(field_type, 'STRING') #默认映射为STRING
        create_sql += f"{field_name} {hive_type},"
    create_sql = create_sql.rstrip(',') + ")"
    create_sql += f"COMMENT '{dbName}.{tableName}表' \nROW FORMAT DELIMITED FIELDS TERMINATED BY ',';\n"
    print(create_sql)
    return create_sql

if __name__ == '__main__':
    #校验外部参数
    if len(sys.argv) != 3:
        print("请传入数据库名与表名")
    dbName = sys.argv[1]
    tableName = sys.argv[2]
    print("数据库名：%s,表名：%s" % (dbName, tableName))
    #获取数据库表信息
    field_info = getDBDate(dbName, tableName)
    #生成建表语句
    create_sql = createTableSql(dbName, tableName, field_info)
    with open("./hive_create_table.sql","a",encoding="utf-8") as f:
        f.write(create_sql)