# -*- coding: utf-8 -*-

# Builtin Modules
import re
import traceback

# 3rd-party Modules
import six
import pymysql
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB

SQL_PARAM_ESCAPE_MAP = {
  '\0'  : '\\0',
  '\b'  : '\\b',
  '\t'  : '\\t',
  '\n'  : '\\n',
  '\r'  : '\\r',
  '\x1a': '\\Z',
  '"'   : '\\"',
  '\''  : '\\\'',
  '\\'  : '\\\\',
}

class HexStr(str):
    pass

def escape_sql_param(s):
    if s is None:
        return 'NULL'

    elif s in (True, False):
        s = str(s)
        s = s.upper()
        return s

    elif isinstance(s, six.string_types):
        is_hex_str = isinstance(s, HexStr)

        s = six.ensure_str(s)
        s = ''.join(SQL_PARAM_ESCAPE_MAP.get(c, c) for c in list(s))
        s = "'{}'".format(six.ensure_str(s))
        if is_hex_str:
            s = 'X' + s
        return s

    elif isinstance(s, (six.integer_types, float)):
        return str(s)

    else:
        s = str(s)
        s = "'{}'".format(s)
        return s

def format_sql(sql, sql_params=None, pretty=False):
    # Inspired by https://github.com/mysqljs/sqlstring/blob/master/lib/SqlString.js
    if not sql_params:
        return sql

    if not isinstance(sql_params, (list, tuple)):
        sql_params = [sql_params]

    result          = ''
    placeholder_re  = re.compile('\?+', re.M)
    chunk_index     = 0
    sql_param_index = 0

    for m in re.finditer(placeholder_re, sql):
        if sql_param_index >= len(sql_params):
            break

        placeholder = m.group()
        if len(placeholder) > 2:
            continue

        sql_param = sql_params[sql_param_index]

        escaped_sql_param = str(sql_param)
        if placeholder == '?':
            if isinstance(sql_param, dict):
                # Dict -> field = 'Value', ...
                expressions = []
                for k, v in sql_param.items():
                    if v is None:
                        expressions.append('{} = NULL'.format(k))

                    else:
                        expressions.append("{} = {}".format(k, escape_sql_param(v)))

                escaped_sql_param = (',\n  ' if pretty else ', ').join(expressions)

            elif isinstance(sql_param, (tuple, list, set)):
                # Tuple, List -> 'value1', 'value2', ...
                expressions = []
                for x in sql_param:
                    if isinstance(x, (tuple, list, set)):
                        values = [escape_sql_param(v) for v in x]
                        expressions.append('({})'.format(', '.join(values)))

                    else:
                        expressions.append(escape_sql_param(x))

                escaped_sql_param = (',\n  ' if pretty else ', ').join(expressions)

            else:
                # Other -> 'value'
                escaped_sql_param = escape_sql_param(sql_param)

        start_index, end_index = m.span()
        result += sql[chunk_index:start_index] + escaped_sql_param
        chunk_index = end_index
        sql_param_index += 1

    if chunk_index == 0:
        return sql

    if chunk_index < len(sql):
        return result + sql[chunk_index:]

    return result.strip()

def get_config(c):
    _charset = c.get('charset') or 'utf8mb4'

    config = {
        'host'    : c.get('host') or '127.0.0.1',
        'port'    : int(c.get('port')) or 3306,
        'user'    : c.get('user'),
        'password': c.get('password'),
        'database': c.get('database'),

        'cursorclass'   : DictCursor,
        'charset'       : _charset,
        'init_command'  : 'SET NAMES "{0}"'.format(_charset),
        'maxconnections': 2,
    }
    return config

class MySQLHelper(object):
    def __init__(self, config, *args, **kwargs):
        self.skip_log = True

        self.config = config
        self.client = PooledDB(pymysql, **get_config(config))

    def check(self):
        try:
            self.query('SELECT 1')

        except Exception as e:
            for line in traceback.format_exc().splitlines():
                print(line)

            raise Exception(str(e))

    def start_trans(self):
        if not self.skip_log:
            print('[MYSQL] Trans START')

        conn = self.client.connection()
        cur  = conn.cursor()

        trans_conn = {
            'conn': conn,
            'cur' : cur,
        }

        return trans_conn

    def commit(self, trans_conn):
        if not trans_conn:
            return

        if not self.skip_log:
            print('[MYSQL] Trans COMMIT')

        conn = trans_conn.get('conn')
        cur  = trans_conn.get('cur')

        conn.commit()

        cur.close()
        conn.close()

    def rollback(self, trans_conn):
        if not trans_conn:
            return

        if not self.skip_log:
            print('[MYSQL] Trans ROLLBACK')

        conn = trans_conn.get('conn')
        cur  = trans_conn.get('cur')

        conn.rollback()

        cur.close()
        conn.close()

    def _trans_execute(self, trans_conn, sql, sql_params=None):
        formatted_sql = format_sql(sql, sql_params)

        if not self.skip_log:
            print('[MYSQL] Trans Query `{}`'.format(re.sub('\s+', ' ', formatted_sql, flags=re.M)))

        if not trans_conn:
            raise Exception('Transaction not started')

        conn = trans_conn['conn']
        cur  = trans_conn['cur']

        count  = cur.execute(formatted_sql)
        db_res = cur.fetchall()

        return list(db_res), count

    def _execute(self, sql, sql_params=None):
        formatted_sql = format_sql(sql, sql_params)

        if not self.skip_log:
            print('[MYSQL] Query `{}`'.format(re.sub('\s+', ' ', formatted_sql, flags=re.M)))

        conn = None
        cur  = None

        try:
            conn = self.client.connection()
            cur  = conn.cursor()

            count  = cur.execute(formatted_sql)
            db_res = cur.fetchall()

        except Exception as e:
            for line in traceback.format_exc().splitlines():
                print(line)

            if conn:
                conn.rollback()

            raise

        else:
            conn.commit()

            return list(db_res), count

        finally:
            if cur:
                cur.close()

            if conn:
                conn.close()

    def trans_query(self, trans_conn, sql, sql_params=None):
        result, count = self._trans_execute(trans_conn, sql, sql_params)
        return result

    def trans_non_query(self, trans_conn, sql, sql_params=None):
        result, count = self._trans_execute(trans_conn, sql, sql_params)
        return count

    def query(self, sql, sql_params=None):
        result, count = self._execute(sql, sql_params)
        return result

    def non_query(self, sql, sql_params=None):
        result, count = self._execute(sql, sql_params)
        return count

    def dump_for_json(self, val):
        '''
        Dump JSON to string
        '''
        return toolkit.json_dumps(val)
