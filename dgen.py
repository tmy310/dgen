# -*- coding: utf-8 -*-
"""
データアクセス用のコード生成
"""

import os
import mysql.connector
from argparse import *
import subprocess

class Dgen:

    def __init__(self):
        self.build_argument_parser()
        self.COMMON_BASE = 'dao.'

    def build_argument_parser(self):
        self.parser = ArgumentParser(description='generate source code of database')
        self.parser.add_argument('--host',help='enter database hostname')
        self.parser.add_argument('--db_name',help='enter database name')
        self.parser.add_argument('--user',help='enter user name')
        self.parser.add_argument('--passwd',help='enter password')

    def do_cmd(self,cmd):
        print cmd
        args = cmd.split(' ')
        p = subprocess.Popen(args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                       #  shell=True)
                         shell=False)
        returns = {'return':p.wait()
                   ,'stdout':p.stdout.readlines()
                   ,'stderr':p.stderr.readlines()
                   }
        return returns

    def execute(self):
        # ディレクトリ初期化
        output_base_dir = 'dao/'
        cmd = 'rm -fr ' + output_base_dir
        self.do_cmd(cmd)

        # 対象テーブルリスト取得
        args = self.parser.parse_args()
        conn = mysql.connector.connect(host = args.host, user = args.user, password = args.passwd)
        cursor = conn.cursor()
        cursor.execute("SELECT table_schema, table_name FROM information_schema.tables where table_schema = '" + args.db_name + "'")
        rows = cursor.fetchall()

        # ディレクトリ作成
        dir = output_base_dir + '/'
        cmd = 'mkdir -p ' + dir
        self.do_cmd(cmd)
        self.make_init_src(dir)

        for row in rows:
            schema = row[0]
            table = row[1]
            dir = output_base_dir + schema + '/'
            cmd = 'mkdir -p ' + dir
            self.do_cmd(cmd)
            # todo:追記にする。
            #self.make_include_include_src(output_base_dir,schema)
            self.make_init_src(dir)
            self.make_include_src(output_base_dir, schema, table)

            to_file = self.make_src(schema,table) 

            filename = dir + table + '.py'
            print filename

            fp = open(filename,'w')
            fp.write(to_file)
            fp.close()

    # テーブル定義取得処理
    # 共通処理
    def find_table_def_by_table(self,schema,table_name):
        sql = """
select 
 * 
from 
 information_schema.columns 
where 
 table_name='%s' 
 and 
 table_schema='%s'
order by 
 ordinal_position;
""" % (table_name,schema)
        args = self.parser.parse_args()
        conn = mysql.connector.connect(host = args.host, user = args.user, password = args.passwd)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    # テーブル定義取得処理
    # 共通処理
    def find_cols_by_table(self,schema,table_name):
        rows = self.find_table_def_by_table(schema,table_name)
        cols = []
        for row in rows:
            cols.append(row[3])
        return cols

    def find_update_col(self,cols):
        if 'log_date' in cols:
           return 'log_date'
        if 'last_update' in cols:
           return 'last_update'
        if 'log_time' in cols:
           return 'log_time'
        if 'create_time' in cols:
           return 'create_time'
        return ''
    def make_src(self,schema,table):
            to_file = self.get_src_template()
            to_file = to_file.replace('%REP_TABLE_NAME%',table)
            to_file = to_file.replace('%REP_SCHEMA_NAME%',schema)

            # メソッド作成
            cols = self.find_cols_by_table(schema,table)
            cols = self.pre_process(cols)
            print "cols"
            print cols
            insert_vals = []
            converted_assinged_word_cols = []
            extra_sql = ''
            for col in cols:
              insert_vals.append("'%s'")
              src_delete_template = self.get_src_delete_template()
              src_delete_template = src_delete_template.replace('%REP_DELETE_COL%',col)
              extra_sql += src_delete_template

              col2 = self.convert_assinged_word(col)
              converted_assinged_word_cols.append(col2)
#              print extra_sql
            extra_sql += self.get_src_find_by_where_template()

            rep_insert_col_param = ','.join(cols)
            rep_insert_col = ','.join(insert_vals)
            to_file = to_file.replace('%REP_INSERT_COL_PARAM%',rep_insert_col_param)
            rep_insert_col_def_param = ','.join(converted_assinged_word_cols)
            to_file = to_file.replace('%REP_INSERT_COL_DEF_PARAM%',rep_insert_col_def_param)

            src = ''
            rep_insert_if2 = ''
            rep_duplicate_if = ''
            i = 0
            for insert_col_param in converted_assinged_word_cols:
              src += """
        if %s == None:
          arrs.append('%s')
        else:
          arrs.append("'%s'")
""" % (insert_col_param,'%s','%s')

              rep_insert_if2 += """
        if %s != None:
            to_cols.append('%s')
            to_vals.append("'"+str(%s)+"'")
""" % (insert_col_param,cols[i],insert_col_param)

              rep_duplicate_if += """
        if %s != None:
            to_cols.append("%s = '"+str(%s)+"'")
""" % (insert_col_param,insert_col_param,cols[i])

              i+=1

            to_file = to_file.replace('%REP_INSERT_IF%',src)
            to_file = to_file.replace('%REP_INSERT_IF2%',rep_insert_if2)

            to_file += extra_sql

            class_name = self.make_class_name(table)
            print class_name
            to_file = to_file.replace('%REP_CLASS_NAME%',class_name)

            update_col =  self.find_update_col(cols)
            to_file = to_file.replace('%REP_UPDATE_COL%',update_col)

            return to_file
    def pre_process(self,cols):
       rets = []
       for col in cols:
           rets.append(col.replace(' ','_'))
       return rets

    def convert_assinged_word(self,word):
        ret = word 
        import keyword
        if word in keyword.kwlist:
            ret += '2'
        return ret

    def pre_process(self,cols):
       rets = []
       for col in cols:
           rets.append(col.replace(' ','_'))
       return rets
    # __init__.pyを作成します。
    def make_init_src(self,dir):
            to_file = ''
            fp = open(dir + '__init__.py','w')
            fp.write(to_file)
            fp.close()
    # include.pyを作成します。
    def make_include_src(self,dir,schema,table):
            to_file = self.get_include_template()
            rep_str = ''
            rep_str += 'from %s%s.%s import *' % (self.COMMON_BASE,schema,table) + "\n"
            to_file = to_file.replace('%REP_IMPORTS%',rep_str)

            fp = open(dir + 'include.py','a')
            fp.write(to_file)
            fp.close()
    def make_include_include_src(self,dir,schema):
            to_file = self.get_include_template()
            rep_str = ''
            rep_str += 'from %s%s.include import *' % (self.COMMON_BASE,schema) + "\n"
            to_file = to_file.replace('%REP_IMPORTS%',rep_str)
            fp = open(dir + 'include.py','a')
            fp.write(to_file)
            fp.close()
    def get_include_template(self):
        str ="""
%REP_IMPORTS%
"""
        #str ="""
## -*- coding: utf-8 -*-
#
#%REP_IMPORTS%
#"""
        return str
    def get_src_template(self):
        str ="""
# -*- coding: utf-8 -*-

import mysql.connector

class %REP_CLASS_NAME%:

    def  __init__(self,host,uid,pwd):
        self.table_name='%REP_TABLE_NAME%'
        self.schema='%REP_SCHEMA_NAME%'
        self.host=host
        self.user=uid
        self.passwd=pwd

    def truncate(self):
        sql = 'truncate table ' + self.schema + '.'+  self.table_name
        self.execute_query(sql)

    def clean(self):
        sql = 'delete from ' + self.schema + '.'+  self.table_name + ' where %REP_UPDATE_COL% <= current_date - 180 '
        self.execute_query(sql)

    def insert(self,%REP_INSERT_COL_DEF_PARAM%):
        sql = 'insert into ' + self.schema + '.'+  self.table_name
        sql += '(%REP_INSERT_COL_PARAM%)'
        sql += ' values('
        arrs = []
        to_sql = ''
%REP_INSERT_IF%        
#        sql += "%REP_INSERT_COL%"
        to_sql = ",".join(arrs)
        sql += to_sql

        sql += ')' 
        sql = sql % (%REP_INSERT_COL_DEF_PARAM%)
        conn = mysql.connector.connect(host = self.host, user = self.user, password = self.passwd)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.execute('commit')

    def insert_without_none(self,%REP_INSERT_COL_DEF_PARAM%):
        sql = 'insert into ' + self.schema + '.'+  self.table_name

        to_cols = []
        to_vals = []

%REP_INSERT_IF2%        
        sql += '('+','.join(to_cols)+')'
        sql += ' values('
        sql += ','.join(to_vals)
        sql += ')'
        conn = mysql.connector.connect(host = self.host, user = self.user, password = self.passwd)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.execute('commit')
"""
        return str

    def get_src_delete_template(self):
        str ="""
    def delete_by_%REP_DELETE_COL%(self,param):
        sql = 'delete from ' + self.schema +'.' + self.table_name + " where %REP_DELETE_COL% = '%s'; " % (param)
        conn = mysql.connector.connect(host = self.host, user = self.user, password = self.passwd)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.execute('commit')

    def find_by_%REP_DELETE_COL%(self,param):
        sql = 'select * from ' + self.schema +'.' + self.table_name + " where %REP_DELETE_COL% = '%s'; " % (param)
        conn = mysql.connector.connect(host = self.host, user = self.user, password = self.passwd)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    def update_by_%REP_DELETE_COL%(self,param):
        sql = 'update ' + self.schema +'.' + self.table_name + " set where %REP_DELETE_COL% = '%s'; " % (param)
        conn = mysql.connector.connect(host = self.host, user = self.user, password = self.passwd)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.execute('commit')
"""
        return str

    def get_src_find_by_where_template(self):
        str ="""
    def find_by_where(self,where,order):
        sql = 'select * from ' + self.schema +'.' + self.table_name + " where %s order by %s" % (where,order)
        conn = mysql.connector.connect(host = self.host, user = self.user, password = self.passwd)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    def find_all(self,order):
        sql = 'select * from ' + self.schema +'.' + self.table_name + " order by %s" % (order)
        conn = mysql.connector.connect(host = self.host, user = self.user, password = self.passwd)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
"""
        return str
    def make_class_name(self,str):
        strs = str.split('_')
        print "make_class_name:"+str
        print strs
        ret = ''
        for part in strs:
          if part == '':
             continue
          buff = part[0].upper() 
          if len(part) >= 2:
              buff += part[1:]
          ret += buff
        return ret
    def get_src_is_duplicate(self):
        str ="""
    def is_duplicate(self,%REP_INSERT_COL_DEF_PARAM%):
        sql = 'select * from ' + self.schema + '.'+  self.table_name
        to_cols = []
        to_sql = ' where '
%REP_INSERT_IF%        
        to_sql += " and ".join(to_cols)
        sql += to_sql

        cursor = self.execute_query(sql)
        rows = cursor.fetchall()
        for row in rows:
          return True
        return False

"""
        return str

if __name__ == '__main__':
    dgen = Dgen()
    dgen.execute()

