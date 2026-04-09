import json
import os
import sys
import pymysql
from pymysql.cursors import DictCursor as sqlDictCursor

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
    #print(result_lower)
    curses.close()
    conn.close()
    return result_lower

def getAllCloudDBName(field_info):
    mapstr=map(lambda x:x['column_name'],field_info)
    return ",".join(list(mapstr))

def getAllClumnsNameAndType(result):
    # 获取列的名字和类型
    mappings = {
        'bigint': 'bigint',
        'varchar': 'string',
        'int': 'int',
        'datetime': 'string',
        'text': 'string',
        'decimal': 'string',
        'date': 'string',
        'timestamp': 'string',
        'varbinary': 'Bytes',
        'double': 'double',
        'time': 'Date'
    }

    list2=list(map(lambda x:{"name":x['column_name'],"type":mappings.get(x['data_type'],'string')},result))
    return list2


if __name__ == '__main__':
    #校验外部参数
    if len(sys.argv) != 3:
        print("请传入数据库名与表名")
    dbName = sys.argv[1]
    tableName = sys.argv[2]
    #获取数据库表信息
    field_info = getDBDate(dbName, tableName)
    column=getAllCloudDBName(field_info)
    columnAndType= getAllClumnsNameAndType(field_info)

    jsonData = {
        "job": {
            "setting": {
                "speed": {
                    "channel": 3
                },
                "errorLimit": {
                    "record": 0,
                    "percentage": 0.02
                }
            },
            "content": [
                {
                    "reader": {
                        "name": "mysqlreader",
                        "parameter": {
                            "username": "root",
                            "password": "592251@Ks",
                            "connection": [
                                {
                                    "querySql": [
                                        f"select {column} from {tableName}"
                                    ],
                                    "jdbcUrl": [
                                        f"jdbc:mysql://node1:3306/{dbName}"
                                    ]
                                }
                            ]
                        }
                    },
                    "writer": {
                        "name": "hdfswriter",
                        "parameter": {
                            "defaultFS": "hdfs://node1:9820",
                            "fileType": "text",
                            "path": f"/user/hive-4.0.1/warehouse/finance.db/ods_jrxd_{tableName}",
                            "fileName": f"{tableName}",
                            "writeMode": "append",
                            "column":
                                columnAndType
                            ,
                            "fieldDelimiter": ","
                        }
                    }
                }
            ]
        }
    }

os.makedirs("./datax_json", exist_ok=True)
with open(f"./datax_json/{tableName}.json", "w") as f:
        json.dump(jsonData, f)