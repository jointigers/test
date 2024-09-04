from datetime import datetime
from elasticsearch import Elasticsearch
import mysql.connector
from mymodel import MySQLConnector
from mymodel import ESConnector
import logging
import re
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
connector = MySQLConnector.MySQLConnector(host='127.0.0.1', user='root', password='Uck123', database='es')
#打算做快照的索引
index_list=['prod-*','shrink-*']
es = ESConnector.ESConnector(hosts="http://elastic:xxxxx@10.184.4.111:9200/")
#获取索引
indexs=es.getIndexs(index_list)
#索引写入数据库
for i in indexs:
    sql="insert into es(index_name,index_createtime) values ('%s','%s')" %(i['index_name'],i['creation_time'])
    try:
        connector.executeInsert(sql)
        body = {
        "indices": i['index_name'],
        "ignore_unavailable": True,
        "include_global_state": False
        }
        snapshot_name=f"{i['index_name']}_{'sp'}"
    except Exception as e:
        logging.warning(str(e))
#对N天前未做快照的索引进行查询，并做快照
try:
    result = connector.executeQuery('SELECT * FROM es where index_createtime < (UNIX_TIMESTAMP()*1000 - (10 * 24 * 60 * 60 * 1000 )) and is_sp=0')
    for row in result:
        body = {
        "indices": row[1],
        "ignore_unavailable": True,
        "include_global_state": False
        }
        snapshot_name=f"{row[1]}_{'sp'}"
        sql="UPDATE es SET is_sp = '1' WHERE index_name='%s'" %row[1]
        try:
            create_res=es.createSnapshot(repository='my_s3_repo', snapshot=snapshot_name, body=body)
            if create_res['accepted']:
                 connector.executeUpdate(sql)
        except Exception as e:
            logging.warning(str(e))
            if re.search(r'snapshot_name_already_in_use_exception', str(e)):
                connector.executeUpdate(sql)
            pass
except Exception as e:
    logging.warning(str(e))
#对N天前的索引进行关闭，防止大的查询
#try:
#    result = connector.executeQuery('SELECT * FROM es where index_createtime < (UNIX_TIMESTAMP()*1000 - (17 * 24 * 60 * 60 * 1000 )) and is_sp=1 and index_status=0')
#    for row in result:
#        closestatus=es.executeClose(row[1])
#        if closestatus:
#            sql="UPDATE es SET index_status = '1' WHERE index_name='%s'" %row[1]
#            connector.executeUpdate(sql)
#except Exception as e:
#    logging.warning(str(e))
