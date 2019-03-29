#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
アパッチログデータ登録用のスクリプトになります。
"""

import os
import set_path_lib
from anz.common.anz_init import *
AnzInit.init(os.path.basename(__file__))


#from include_lib import *
from anz.contents.local.include import *
import os
from argparse import *

class ApacheRestoretor:

    def get_title(self):
        args = self.parser.parse_args()
        return args.contents_id

    def __init__(self):
        #self.COMMON_BASE = 'anz.common.dao.'
        self.COMMON_BASE = ''
        self.build_argument_parser()
     
        contents_id = self.get_title()

    def build_argument_parser(self):
        self.parser = ArgumentParser(description='restore apachelog')
        self.parser.add_argument('contents_id',help='contents_id ex:swcn')
        self.parser.add_argument('--dbms',help='contents_id ex:swcn')
    
    def execute(self,exec_filename):
        args = self.parser.parse_args()
        schema = self.get_title()
        table = ''

        if args.dbms == None:
           args.dbms='greenplum_db_base'
        dbms_config_list = AnzProperty.get(['DB'])
        db_list = dbms_config_list[args.dbms.upper()]
        self.gdb = AnzDb(db_list['NAME'],db_list['HOST'],db_list['USER'],db_list['PASS'],args.dbms)

        output_base_dir = 'dao/'
        cmd = 'rm -fr ' + output_base_dir
        Util.do_cmd(cmd)
  
        list = self.gdb.find_all_table_name()

        dir = output_base_dir + '/'
        cmd = 'mkdir -p ' + dir
        Util.do_cmd(cmd)
        self.make_init_src(dir)

        for schema,tables in list.items():
          #publioは除外
          if schema == 'public' or schema == 'ppsn':
            continue
          dir = output_base_dir + schema + '/'
          cmd = 'mkdir -p ' + dir
          Util.do_cmd(cmd)
          # todo:追記にする。
          #self.make_include_include_src(output_base_dir,schema)
          self.make_init_src(dir)
          self.make_include_src(output_base_dir,schema,tables)

          for table in tables:

            if table.find('return')==0 or table.find('yellowfin')>=0:
              continue
            to_file = self.make_src(schema,table) 
#            print to_file

            filename = dir + table + '.py'
            print filename

            fp = open(filename,'w')
            fp.write(to_file)
            fp.close()

#          sys.exit()

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
            cols = self.gdb.find_cols_by_table(schema,table)
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
            extra_sql += self.get_src_bulkload_template()
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
#            to_file = to_file.replace('%REP_INSERT_COL%',rep_insert_col)

            src_is_duplicate = self.get_src_is_duplicate()
            src_is_duplicate = src_is_duplicate.replace('%REP_INSERT_IF%',rep_duplicate_if)
            src_is_duplicate = src_is_duplicate.replace('%REP_INSERT_COL_DEF_PARAM%',rep_insert_col_def_param)

            to_file += extra_sql
            to_file += src_is_duplicate

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
    def make_include_src(self,dir,schema,tables):
            to_file = self.get_include_template()
            rep_str = ''
            for table in tables:
               if table.find('return')==0 or table.find('yellowfin')>=0:
                continue
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

from anz.common.db.db_strategy import *

class %REP_CLASS_NAME%(DbStrategy):

    def  __init__(self,db_name,host,uid,pwd,dbms):
        table_name='%REP_TABLE_NAME%'
        self.table_name='%REP_TABLE_NAME%'
        schema='%REP_SCHEMA_NAME%'
        self.schema='%REP_SCHEMA_NAME%'
        DbStrategy.__init__(self,db_name,schema,host,uid,pwd,table_name,dbms)

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
        self.execute_query(sql)

    def insert_without_none(self,%REP_INSERT_COL_DEF_PARAM%):
        sql = 'insert into ' + self.schema + '.'+  self.table_name

        to_cols = []
        to_vals = []

%REP_INSERT_IF2%        
        sql += '('+','.join(to_cols)+')'
        sql += ' values('
        sql += ','.join(to_vals)
        sql += ')'
        self.execute_query(sql)

"""
        return str

    def get_src_delete_template(self):
        str ="""
    def delete_by_%REP_DELETE_COL%(self,param):
        sql = 'delete from ' + self.schema +'.' + self.table_name + " where %REP_DELETE_COL% = '%s'; " % (param)
        self.execute_query(sql)

    def find_by_%REP_DELETE_COL%(self,param):
        sql = 'select * from ' + self.schema +'.' + self.table_name + " where %REP_DELETE_COL% = '%s'; " % (param)
        cursor = self.execute_query(sql)
        rows = cursor.fetchall()
        return rows

    def update_by_%REP_DELETE_COL%(self,param):
        sql = 'update ' + self.schema +'.' + self.table_name + " set where %REP_DELETE_COL% = '%s'; " % (param)
        cursor = self.execute_query(sql)
        rows = cursor.fetchall()
        return rows
"""
        return str

    def get_src_bulkload_template(self):
        str ="""
    def bulkload(self,csvfile,ymlfile='',format='CSV',delimiter=None,quote='"',log_file=None,is_delete_yml=True,mode='insert'):
        self.cmd_gpload(csvfile,ymlfile=ymlfile,format=format,delimiter=delimiter,quote=quote,log_file=log_file,is_delete_yml=is_delete_yml,mode=mode)
"""
        return str

    def get_src_find_by_where_template(self):
        str ="""
    def find_by_where(self,where,order):
        sql = 'select * from ' + self.schema +'.' + self.table_name + " where %s order by %s" % (where,order)
        cursor = self.execute_query(sql)
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
    exec_file_name=os.path.basename(__file__)
    ar = ApacheRestoretor()
    ar.execute(exec_file_name)

