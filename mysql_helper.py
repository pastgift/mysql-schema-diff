# -*- coding: utf-8 -*-

import datetime

import MySQLdb
import MySQLdb.converters as converters
from MySQLdb.cursors import DictCursor
from DBUtils.PooledDB import PooledDB

conv = converters.conversions.copy()
conv[MySQLdb.FIELD_TYPE.DATE] = str
conv[MySQLdb.FIELD_TYPE.DATETIME] = str
conv[MySQLdb.FIELD_TYPE.TIMESTAMP] = str

def mysql_escape_string(s):
    if isinstance(s, str):
        return MySQLdb.escape_string(s)
    elif isinstance(s, unicode):
        return MySQLdb.escape_string(s.encode('utf8'))
    else:
        return str(s)

def sql_params_replace(sql, sql_params):
    if not sql_params:
        return sql
    for p in sql_params:
        if '?' in sql:
            if sql[sql.index('?') + 1] == '?':
                # 存在参数占位符，且为不转义占位符
                sql = sql.replace('??', str(p), 1)

            else:
                # 存在参数占位符，且为转义占位符
                if p is None:
                    # None，强制转换为 NULL
                    sql = sql.replace('?', 'NULL', 1)

                elif isinstance(p, dict):
                    # 字典类型，转换为 a = '1', b = '2' ...
                    expressions = []
                    for k, v in p.items():
                        if v is None:
                            expressions.append("{} = NULL".format(k))
                        elif isinstance(v, (int, long, float)):
                            expressions.append("{} = {}".format(k, v))
                        else:
                            expressions.append("{} = '{}'".format(k, mysql_escape_string(v)))
                    sql_part = ', '.join(expressions)
                    sql = sql.replace('?', sql_part, 1)

                elif isinstance(p, list):
                    # 列表类型，转换为 1, 2, 3 ...
                    expressions = []
                    for x in p:
                        if x is None:
                            expressions.append('NULL')
                        elif isinstance(x, (int, long, float)):
                            expressions.append(x)
                        else:
                            expressions.append("'{}'".format(mysql_escape_string(x)))

                    sql_part = ', '.join(expressions)
                    sql = sql.replace('?', sql_part, 1)

                else:
                    # 其他，直接转换为字符串
                    sql = sql.replace('?', "'{}'".format(mysql_escape_string(str(p))), 1)

    return sql

def prepare_py_str(db_result):
    for row in db_result:
        for (k, v) in row.items():
            if isinstance(v, datetime.datetime):
                row[k] = str(v)

            elif isinstance(v, str):
                return unicode(v.decode('utf8'))

    return db_result

class MySQLHelper(object):
    def __init__(self, host=None, port=3306, user=None, passwd=None, db=None, charset='utf8', maxconnections=5):
        self.option = {
            'host'          : host,
            'port'          : int(port),
            'user'          : user,
            'passwd'        : passwd,
            'db'            : db,
            'maxconnections': int(maxconnections),
            'charset'       : charset,
            'cursorclass'   : DictCursor,
        }
        self.mysql_pool = PooledDB(MySQLdb, **self.option)

    def start_trans(self):
        conn = self.mysql_pool.connection()
        cur  = conn.cursor()

        trans_conn = {
            'conn': conn,
            'cur' : cur,
        }

        return trans_conn

    def commit(self, trans_conn):
        conn = trans_conn['conn']
        cur  = trans_conn['cur']

        conn.commit()
        cur.close()
        conn.close()

    def rollback(self, trans_conn):
        conn = trans_conn['conn']
        cur  = trans_conn['cur']

        conn.rollback()
        cur.close()
        conn.close()

    def trans_query(self, trans_conn, sql, sql_params=None):
        sql = sql_params_replace(sql, sql_params)

        conn = trans_conn['conn']
        cur  = trans_conn['cur']

        db_result = None

        cur.execute(sql)

        db_result = cur.fetchall()
        db_result = prepare_py_str(db_result)

        return db_result

    def query(self, sql, sql_params=None):
        sql = sql_params_replace(sql, sql_params)

        db_result = None

        conn = self.mysql_pool.connection()
        cur  = conn.cursor()

        cur.execute(sql)

        db_result = cur.fetchall()
        db_result = prepare_py_str(db_result)

        conn.commit()
        cur.close()
        conn.close()

        return db_result
