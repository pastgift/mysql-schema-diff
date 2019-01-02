# -*- coding: utf-8 -*-

import sys
import json
import re

from collections import OrderedDict
from mysql_helper import MySQLHelper

COLOR_RED    = '\033[1;31m'
COLOR_GREEN  = '\033[1;32m'
COLOR_YELLOW = '\033[1;33m'
COLOR_BLUE   = '\033[1;34m'
COLOR_PURPLE = '\033[1;35m'
COLOR_CYAN   = '\033[1;36m'
COLOR_GRAY   = '\033[1;37m'
COLOR_WHITE  = '\033[1;38m'
COLOR_RESET  = '\033[1;0m'

COLUMN_PROPS = [
    'TABLE_CATALOG',
    # 'TABLE_SCHEMA',
    'TABLE_NAME',
    'COLUMN_NAME',
    'ORDINAL_POSITION',
    'COLUMN_DEFAULT',
    'IS_NULLABLE',
    'DATA_TYPE',
    'CHARACTER_MAXIMUM_LENGTH',
    'CHARACTER_OCTET_LENGTH',
    'NUMERIC_PRECISION',
    'NUMERIC_SCALE',
    'DATETIME_PRECISION',
    'CHARACTER_SET_NAME',
    'COLLATION_NAME',
    'COLUMN_TYPE',
    'COLUMN_KEY',
    'EXTRA',
    'COLUMN_COMMENT',
]

def get_mysql_option(conn_str):
    conn_str = conn_str.replace('mysql://', '')

    mysql_option = OrderedDict()
    mysql_option['host']   = None
    mysql_option['user']   = None
    mysql_option['passwd'] = None
    mysql_option['db']     = None

    auth_part = None
    url_part  = None

    if '@' in conn_str:
        auth_part, url_part = conn_str.split('@')
    else:
        url_part = conn_str

    if auth_part:
        if ':' in auth_part:
            mysql_option['user'], mysql_option['passwd'] = auth_part.split(':')
        else:
            mysql_option['user'] = auth_part

    if url_part:
        if '/' in url_part:
            host_part, mysql_option['db'] = url_part.split('/')

            if ':' in host_part:
                mysql_option['host'], mysql_option['port'] = host_part.split(':')

            else:
                mysql_option['host'] = host_part

        else:
            raise Exception('No database specified')

    else:
        raise Exception('Invalid MySQL connection string')

    return mysql_option

def get_mysql_schema(db):
    '''
    返回结构如下：
        {
            "<tableName>": {
                "syntax": <str>,
                "columns": {
                    "<columnName>": {
                        "TABLE_CATALOG"           : <value>,
                        "ORDINAL_POSITION"        : <value>,
                        "COLUMN_DEFAULT"          : <value>,
                        "IS_NULLABLE"             : <value>,
                        "DATA_TYPE"               : <value>,
                        "CHARACTER_MAXIMUM_LENGTH": <value>,
                        "CHARACTER_OCTET_LENGTH"  : <value>,
                        "NUMERIC_PRECISION"       : <value>,
                        "NUMERIC_SCALE"           : <value>,
                        "DATETIME_PRECISION"      : <value>,
                        "CHARACTER_SET_NAME"      : <value>,
                        "COLLATION_NAME"          : <value>,
                        "COLUMN_TYPE"             : <value>,
                        "COLUMN_KEY"              : <value>,
                        "EXTRA"                   : <value>,
                        "PRIVILEGES"              : <value>,
                        "COLUMN_COMMENT"          : <value>,
                    }
                }
            }
        }
    '''
    mysql_schemas = OrderedDict()

    # 获取所有表.列结构
    sql = '''
        SELECT
            *
        FROM
            information_schema.columns
        where
            TABLE_SCHEMA = ?
        ORDER BY
            TABLE_NAME,
            ORDINAL_POSITION
        '''
    sql_params = [db.option['db']]
    db_ret = db.query(sql, sql_params)
    for r in db_ret:
        table_name  = r['TABLE_NAME']
        column_name = r['COLUMN_NAME']

        if table_name.startswith('_') or column_name.startswith('_'):
            continue

        if table_name not in mysql_schemas:
            mysql_schemas[table_name] = {
                'syntax' : None,
                'columns': OrderedDict(),
            }

        if column_name not in mysql_schemas[table_name]['columns']:
            mysql_schemas[table_name]['columns'][column_name] = {}

        for p in COLUMN_PROPS:
            mysql_schemas[table_name]['columns'][column_name][p] = r[p]


    # 获取所有建表语句
    for table_name, _ in mysql_schemas.items():
        sql = '''
            SHOW CREATE TABLE `??`
        '''
        sql_params = [table_name]
        db_ret = db.query(sql, sql_params)

        if 'Create Table' in db_ret[0]:
            # 表
            syntax = db_ret[0]['Create Table']
            syntax = re.sub(' AUTO_INCREMENT=\d+', '', syntax)

        elif 'Create View' in db_ret[0]:
            # 视图
            syntax = db_ret[0]['Create View']
            syntax = re.sub(' ALGORITHM=UNDEFINED DEFINER=`[a-zA-Z0-9]+`@`%` SQL SECURITY DEFINER', '', syntax)

        # 去除数据库名，避免影响对比
        syntax = syntax.replace(db.option['db'], '<thisDB>')

        mysql_schemas[table_name]['syntax'] = syntax

    return mysql_schemas

def compare_schema(base_schema, target_schema):
    '''
    返回结构如下：
        {
            "<tableName>": {
                "syntaxChanged": True|False,

                "tableAdded"  : True|False,
                "tableRemoved": True|False,
                "changedColumns": {
                    "<columnName>": {
                        "columnAdded"  : True|False,
                        "columnRemoved": True|False,

                        "columnChanges": {
                            "<columnPorp>": {
                                "base"  : <value>,
                                "target": <value>,
                            }
                        }
                    }
                }
            }
        }
    '''
    diff_schemas = OrderedDict()

    for table_name in list(set(base_schema.keys() + target_schema.keys())):
        base_table  = base_schema.get(table_name)
        target_table = target_schema.get(table_name)

        diff = {
            'tableAdded'    : False,
            'tableRemoved'  : False,
            'syntaxChanged' : False,
            'changedColumns': OrderedDict(),
        }

        if (base_table is None) and (target_table is not None):
            # 表增加
            diff['tableAdded'] = True
            diff_schemas[table_name] = diff

        elif (base_table is not None) and (target_table is None):
            # 表删除
            diff['tableRemoved'] = True
            diff_schemas[table_name] = diff

        else:
            # 继续对比建表语句
            if base_table['syntax'] != target_table['syntax']:
                diff['syntaxChanged'] = True

                # 继续比较各列
                for column_name in list(set(base_table['columns'].keys() + target_table['columns'].keys())):
                    base_column  = base_table['columns'].get(column_name)
                    target_column = target_table['columns'].get(column_name)

                    col_diff = {
                        'columnAdded'  : False,
                        'columnRemoved': False,
                        'columnChanges': OrderedDict(),
                    }

                    if (base_column is None) and (target_column is not None):
                        # 列增加
                        col_diff['columnAdded'] = True
                        diff['changedColumns'][column_name] = col_diff

                    elif (base_column is not None) and (target_column is None):
                        # 列删除
                        col_diff['columnRemoved'] = True
                        diff['changedColumns'][column_name] = col_diff

                    else:
                        # 继续比较各列属性
                        for p in COLUMN_PROPS:
                            if base_column[p] != target_column[p]:
                                col_diff['columnChanges'][p] = {
                                    'base'  : base_column[p],
                                    'target': target_column[p],
                                }

                        if col_diff['columnChanges']:
                            diff['changedColumns'][column_name] = col_diff

                diff_schemas[table_name] = diff

    return diff_schemas

def convert_readable_value(v):
    if v is None:
        return u'NULL'

    elif v == '':
        return u'<空字符串>'

    else:
        return v

def print_schema_diff(schema_diff, no_color=False):
    for table_name, table_diff in schema_diff.items():
        print_line = u'\n'

        line_label = ''
        if table_diff['tableAdded']:
            line_label = u'+ [多余表] '
            if no_color is False:
                line_label = COLOR_GREEN  + line_label + COLOR_RESET

        elif table_diff['tableRemoved']:
            line_label = u'- [缺少表] '
            if no_color is False:
                line_label = COLOR_RED  + line_label + COLOR_RESET

        elif table_diff['syntaxChanged']:
            line_label = u'* [差异表] '
            if no_color is False:
                line_label = COLOR_YELLOW  + line_label + COLOR_RESET

        print_line += line_label + table_name
        print print_line.encode('utf8')

        changed_columns = table_diff['changedColumns']
        if changed_columns:
            for column_name, column_diff in changed_columns.items():
                print_line = u'\t'

                line_label = ''
                if column_diff['columnAdded']:
                    line_label = u'+ [多余列] '
                    if no_color is False:
                        line_label = COLOR_GREEN  + line_label + COLOR_RESET

                elif column_diff['columnRemoved']:
                    line_label = u'- [缺少列] '
                    if no_color is False:
                        line_label = COLOR_RED  + line_label + COLOR_RESET

                elif column_diff['columnChanges']:
                    line_label = u'* [差异列] '
                    if no_color is False:
                        line_label = COLOR_YELLOW  + line_label + COLOR_RESET

                print_line += line_label + column_name
                print print_line.encode('utf8')

                column_changes = column_diff['columnChanges']
                if column_changes:
                    for column_prop, diff_info in column_changes.items():
                        base_value  = convert_readable_value(diff_info['base'])
                        target_value = convert_readable_value(diff_info['target'])

                        print_line = u'\t\t{:-<30} 从基准数据库的`{}`被改为目标数据库的`{}`'.format(
                                u'{} '.format(column_prop),
                                base_value,
                                target_value)
                        print print_line.encode('utf8')

def main():
    db_base_option  = get_mysql_option(sys.argv[1])
    db_target_option = get_mysql_option(sys.argv[2])

    no_color_option = False
    if len(sys.argv) > 3 and sys.argv[3] == '--no-color':
        no_color_option = True

    db_base  = MySQLHelper(**db_base_option)
    db_target = MySQLHelper(**db_target_option)

    if db_base_option['passwd']:
        db_base_option['passwd'] = '***'

    if db_target_option['passwd']:
        db_target_option['passwd'] = '***'

    print '基准数据库:', ', '.join(['{}={}'.format(k, v) for k, v in db_base_option.items()])
    print '目标数据库:', ', '.join(['{}={}'.format(k, v) for k, v in db_target_option.items()])

    db_base_schema  = get_mysql_schema(db_base)
    db_target_schema = get_mysql_schema(db_target)

    schema_diff = compare_schema(db_base_schema, db_target_schema)

    if schema_diff:
        print '\n目标数据库相对于基准数据库存在以下差异：'
        print_schema_diff(schema_diff, no_color_option)

    else:
        print '\n数据库结构完全一致'

if __name__ == '__main__':
    main()