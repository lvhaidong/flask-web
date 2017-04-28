# -*- coding: utf-8 -*-
"""
    #    pysql.py
    #    ~~~~~~~~~~~~~~~~~~~~~~~
    #    定义数据库操作的通用方法，支持MySQL(InnoDB),Oracle以及postgreSQL
    #
    #    @version: 1
"""

# Mysql library
import pymysql
# Oracle Library fo Python
import cx_Oracle
# PostgreSQL library
import psycopg2

from flask import g as flask_g
from utils import config, logger


class CURSOR(object):
    """数据库连接类，支持Oracle与MySQL，具体的数据库连接参数需要在settings.py文件中定义.

    Examples:
        db = CURSOR(dbid='ORACLE_SD')
        items = ['cha_id$sdid', 'status', 'cha_description$description', 'requestor']
        sql = "select # from cmdb.v_smg_changes where @"
        where = {'ass_person_to': u'陈明', 'status': u'打开'}
        res = db.select(items=items, sql=sql, where=where)
        for r in res:
            for k in r.keys():
                print '%s=%s' % (k, r[k])
            print
    """

    def __init__(self, auto_commit=True, global_trans=False, dbid='MYSQL_MAIN'):
        '''CURSOR初始化函数
        @param auto_commit: bool, 标记数据库连接是否自动提交
        @param global_trans: bool, 是否全局事务，默认False，任何子函数调用都处于独立事务内，否则则为同一事务
        @param dbid: string, 标记数据库名称，必须符合格式：MYSQL_XXX,ORACLE_XXX
        @return: None

        '''
        self._g_dbconns = {}  # 本地的全局DB连接缓存
        self._g_dbcursors = {}  # 本地的全局游标缓存
        self._conn = None
        self._cursor = None
        self._fetchencode = None  # 游标fetch返回的结果集编码，默认应该是Unicode
        if global_trans:
            # 全局事务不允许同时存在多个，而且一定不是自动提交
            if dbid in self.g_dbcursors:
                raise Exception('全局事务冲突，代码存在逻辑错误，不能同时申请多个全局事务')
            self._auto_commit = False
            self.g_dbcursors[dbid] = None  # NOTE: 这仅仅是个标志，用于局部事务判断自己是否是全局事务的子事务
        else:
            # 局部事务如果是子事务，那么只能非自动提交
            if dbid in self.g_dbcursors:
                self._auto_commit = False
            else:
                self._auto_commit = auto_commit
        self._global_trans = global_trans
        self._dbid = dbid
        self._dbtype = dbid.split('_')[0]  # 记录数据库类型

    def _connect_db(self):
        '''连接数据库，根据dbid初始化数据库连接：_conn'''
        # 优先读取全局缓存
        if self._dbid in self.g_dbconns:
            self._conn = self.g_dbconns[self._dbid]
            return
            # config 从util.config 方法中读取，使用k-v 格式
        #        if self._dbid == 'MYSQL_TEST':  # 测试数据库
        if 'MYSQL' in self._dbid:
            self._conn = pymysql.connect(
                host=config[self._dbid + '_HOST'],
                port=config[self._dbid + '_PORT'],
                user=config[self._dbid + '_USER'],
                passwd=config[self._dbid + '_PASSWD'],
                db=config[self._dbid + '_DB'],
                charset=config[self._dbid + '_CHARSET'],
                connect_timeout=10  # 10s timeout
            )
        elif 'PGSQL' in self._dbid:  # PostgreSQL Database Connection Method
            if config[self._dbid + '_PORT']:
                port = config[self._dbid + '_PORT']
            else:
                port = 5432
            self._conn = psycopg2.connect(
                host=config[self._dbid + '_HOST'],
                user=config[self._dbid + '_USER'],
                password=config[self._dbid + '_PASSWD'],
                database=config[self._dbid + '_DB'],
                port=config[self._dbid + '_PORT']

            )
        elif 'ORACLE' in self._dbid:  # Service Desk后台数据库
            # 修改NLS_LANG环境变量为UTF8以使cx_Oracle输入输入字符串为utf8编码
            import os
            os.environ['NLS_LANG'] = config['%s_CHARSET' % self._dbid]
            self._fetchencode = 'utf8'  # 指明cx_oracle返回的结果集为utf8编码，后面select时进行解码以变成统一的unicode
            self._conn = cx_Oracle.connect(
                user=config[self._dbid + '_USER'],
                password=config[self._dbid + '_PASSWD'],
                dsn='%s:%s/%s' % (config[self._dbid + '_HOST'],
                                  config[self._dbid + '_PORT'],
                                  config[self._dbid + '_SERVICE'])
            )
            self._conn = cx_Oracle.connect(
                user=config[self._dbid + '_USER'],
                password=config[self._dbid + '_PASSWD'],
                dsn='%s:%s/%s' % (config[self._dbid + '_HOST'],
                                  config[self._dbid + '_PORT'],
                                  config[self._dbid + '_SERVICE'])
            )

        else:
            raise Exception('未知的数据库：%s' % self._dbid)

        # 全局缓存连接
        self.g_dbconns[self._dbid] = self._conn

    def _escape(self, val):
        '转义字符串，MYSQL则转移为`val`，ORACLE则转移为"val"'
        if self._dbtype == 'MYSQL':
            return '`%s`' % val
        elif self._dbtype == 'ORACLE' or self._dbtype == 'PGSQL':
            return '%s' % val
        else:
            raise Exception(u'不支持的数据库类型参数：%s' % self._dbtype)

    def _str_quote(self, data):
        '''返回数据对应的字符串，如果是整数则返回整数字符串，如果是文本则加引号，如：'"xx"' '''
        if isinstance(data, int) or isinstance(data, long):
            return str(data)
        elif isinstance(data, basestring):
            # 将字符串中的引号进行转义，以避免与SQL本身的引号冲突
            data = data.replace("'", "\\'")  # 转义单引号
            data = data.replace('"', '\\"')  # 转义双引号
            if self._dbtype == 'MYSQL':
                return '\'%s\'' % data  # 闫海成修改  修复了sql语句中不支持字符串双引号的情况
            else:
                return '\'%s\'' % data  # oracle字符串为单引号
        elif data is None:  # update value with NULL
            return 'NULL'
        else:
            raise Exception('不支持的数据类型：type=%s,value=%s' % (type(data), data))

    def _kstr_quote(self, data):
        '''返回dict key的字符串值，将形式为't.id'转换为't.`id`'，'id'转换成'`id`'  '''
        if isinstance(data, basestring):
            tmp = data.split('.')
            if len(tmp) == 1:
                return self._escape(data)
            else:
                return '%s.%s' % (tmp[0], self._escape(tmp[1]))
        elif isinstance(data, int):  # 用以支持where 1=1这种条件
            return data
        else:
            raise Exception('错误的KEY数据类型：%s' % type(data))

    def _where_format(self, where):
        '''格式化where字典为WHERE SQL字符串
        @param where: dict, 定义where的字典，如：
                    {'col1':2, 'col2':'xxx', 'col3':[1,2,3], 1:1}，
                list的限定相当于SQL的IN关键字
        @return: str，SQL WHERE子句
        add:
        1. 增加判断，判断key 是否为ALL ，如果 key值为ALL 则返回全部
        '''
        # sql = sql % ('AND'.join([' %s=%s ' % (_kstr_quote(k), _str_quote(where[k])) for k in where.keys()]))
        ws = []
        for k, v in where.items():
            if isinstance(v, list) or isinstance(v, tuple):
                if len(v) > 1:
                    ws.append('%s IN (%s)' % (self._kstr_quote(k),
                                              reduce(lambda m, n: '%s,%s' % (m, self._str_quote(n)), v, '')[1:]
                                              ))
                elif len(v) == 1:  # 列表只有一个元素则改成相等来处理，否则会报语法错误
                    ws.append('%s=%s' % (self._kstr_quote(k), self._str_quote(v[0])))
                else:  # 空列表为FALSE
                    ws.append('1=2')
            else:
                # ws.append('%s=%s' % (self._kstr_quote(k), self._str_quote(v)))
                if v == 'all':  # 如果value 为 'all',则返回全部结果，需要将value修改为key值，以便SQL可以正确执行
                    where[k] = k
                    v = k
                    ws.append('%s=%s' % (self._kstr_quote(k), v))
                else:
                    ws.append('%s=%s' % (self._kstr_quote(k), self._str_quote(v)))
        return reduce(lambda m, n: '%s AND %s' % (m, n), ws)
        ##########fuzzy format

    def _where_fuzzyformat(self, where):
        '''格式化where字典为WHERE SQL字符串
        @param where: dict, 定义where的字典，如：
                    {'col1':2, 'col2':'xxx', 'col3':[1,2,3], 1:1}，
                list的限定相当于SQL的IN关键字
        @return: str，SQL WHERE子句
        '''
        ws = []
        for k, v in where.items():
            if isinstance(v, list) or isinstance(v, tuple):
                if len(v) > 1:
                    ws.append('%s IN (%s)' % (self._kstr_quote(k),
                                              reduce(lambda m, n: '%s,%s' % (m, self._str_quote(n)), v, '')[1:]
                                              ))
                elif len(v) == 1:  # 列表只有一个元素则改成相等来处理，否则会报语法错误
                    ws.append('%s like %s' % (self._kstr_quote(k), self._str_quote('%' + v[0] + '%')))
                else:  # 空列表为FALSE
                    ws.append('1=2')
            else:
                # ws.append('%s=%s' % (self._kstr_quote(k), self._str_quote(v)))
                ws.append('%s like %s' % (self._kstr_quote(k), self._str_quote('%' + v + '%')))
        return reduce(lambda m, n: '%s AND %s' % (m, n), ws)

    ###########
    @property
    def g_dbconns(self):
        '获取全局的DB缓存'
        try:
            return flask_g.dbconns

            # NOTE: 当从网页调用（Flask）的时候缓存使用的是main.before_request函数中的全局缓存
            # 变量，这样能保证一次对话的所有DB连接盒游标是统一的，但当在Flask之外运行时则会报RuntimeError
            # 错误，这里当检测到该错误时则返回本地的全局缓存变量CURSOR._g_dbconns
        except RuntimeError:
            return self._g_dbconns

    @property
    def g_dbcursors(self):
        '获取全局的游标缓存'
        try:
            return flask_g.dbcursors
        except RuntimeError:
            return self._g_dbcursors

    @property
    def conn(self):
        '获取数据库连接'
        if self._conn is None:
            self._connect_db()
        return self._conn

    @property
    def cursor(self):
        '获取游标'
        if self._cursor is None:
            if self._global_trans:  # 全如果是全局事务则新建游标并缓存
                self._cursor = self.conn.cursor()
                self.g_dbcursors[self._dbid] = self._cursor
                logger.debug('全局事务新建：全局游标=%s' % self.g_dbcursors)
            else:  # 对局部事务，如果是子事务则只能复用父游标，否则自建游标
                if self._dbid in self.g_dbcursors:  # 子事务
                    if self.g_dbcursors[self._dbid] is None:
                        self._cursor = self.conn.cursor()
                        self.g_dbcursors[self._dbid] = self._cursor
                        logger.debug('全局事务新建：全局游标=%s' % self.g_dbcursors)
                    else:
                        self._cursor = self.g_dbcursors[self._dbid]
                else:  # 独立局部事务
                    self._cursor = self.conn.cursor()
        return self._cursor

    def commit(self):
        '提交事务'
        if self._global_trans:
            # 对全局事务，提交并销毁全局事务
            tmp = self.conn.commit()
            logger.debug('全局事务提交：全局游标=%s' % self.g_dbcursors)
            self.g_dbcursors.pop(self._dbid)
            self._cursor = None
            return tmp
        elif self._dbid not in self.g_dbcursors:
            return self.conn.commit()

    def rollback(self, message=''):
        '回滚事务'
        if self._global_trans:
            # 对全局事务，回滚并销毁全局事务
            tmp = self.conn.rollback()
            logger.debug('全局事务回滚：全局游标=%s' % self.g_dbcursors)
            self.g_dbcursors.pop(self._dbid)
            self._cursor = None
            return tmp
        elif self._dbid in self.g_dbcursors:
            # 对子事务，不真正回滚，抛出异常由父事务处理
            raise Exception('子事务回滚: %s' % message)
        else:
            # 对独立的局部事务，直接回滚即可
            return self.conn.rollback()

    def auto_commit(self):
        '如果设置了自动提交则提交事务，否则什么也不做'
        if self._auto_commit:
            self.commit()

            #     def close(self):
            #         '''关闭数据库连接'''
            #         try:
            #             self._conn.close()
            #         except:
            #             pass
            #
            #     def __del__(self):
            #         self.close()

    def insert(self, sql, data):
        '''插入数据到数据库
        @param sql: str, insert SQL语句，例如："INSERT INTO tab(&) VALUES(@)"
        @param data: dict, 待插入的数据，例如：{'id':123, 'name':'chen'}
        @return: int, lastrowid

        '''
        assert isinstance(sql, basestring)
        # 对sql语句进行预处理，后期根据不同数据库语法进行相应调整
        sql = sql.replace('@', '%s')
        sql = sql.replace('&', '%s')

        keys = data.keys()
        sql = sql % (','.join([self._escape(k) for k in keys]),
                     ','.join([self._str_quote(data[k]) for k in keys])
                     )
        # 这里将数据的keys与values组合成字符串并拼接到SQL语句中，其中对字符类型（非int/long）
        # 添加了双引号！

        logger.debug(sql)

        try:
            self.cursor.execute(sql)
            self.auto_commit()
            return self.cursor.lastrowid
        except pymysql.err.IntegrityError as e:
            if e.args[0] == 1062:  # Duplicate entry
                raise Exception('数据已存在')

    def insert_return(self, sql, data):
        '''插入数据到数据库
        @param sql: str, insert SQL语句，例如："INSERT INTO tab(&) VALUES(@)"
        @param data: dict, 待插入的数据，例如：{'id':123, 'name':'chen'}
        @return: int, lastrowid

        '''
        assert isinstance(sql, basestring)
        # 对sql语句进行预处理，后期根据不同数据库语法进行相应调整
        sql = sql.replace('@', '%s')
        sql = sql.replace('&', '%s')

        keys = data.keys()
        sql = sql % (','.join([self._escape(k) for k in keys]),
                     ','.join([self._str_quote(data[k]) for k in keys])
                     )
        # 这里将数据的keys与values组合成字符串并拼接到SQL语句中，其中对字符类型（非int/long）
        # 添加了双引号！

        logger.debug(sql)

        try:
            self.cursor.execute(sql)
            item = self.cursor.fetchone()
            self.auto_commit()
            return item
        except pymysql.err.IntegrityError as e:
            if e.args[0] == 1062:  # Duplicate entry
                raise Exception('数据已存在')

    def insert_oracle(self, sql, data):
        '''插入数据到数据库
        @param sql: str, insert SQL语句，例如："INSERT INTO tab(&) VALUES(@)"
        @param data: dict, 待插入的数据，例如：{'id':123, 'name':'chen'}
        @return: int, lastrowid

        '''
        assert isinstance(sql, basestring)
        # 对sql语句进行预处理，后期根据不同数据库语法进行相应调整
        sql = sql.replace('@', '%s')
        sql = sql.replace('&', '%s')

        keys = data.keys()
        sql = sql % (','.join([self._escape(k) for k in keys]),
                     ','.join([self._str_quote(data[k]) for k in keys])
                     )
        # 这里将数据的keys与values组合成字符串并拼接到SQL语句中，其中对字符类型（非int/long）
        # 添加了双引号！

        logger.debug(sql)
        res = self.cursor.execute(sql)
        self.auto_commit()

        return res
        # try:
        #     self.cursor.execute(sql)
        #     self.auto_commit()
        #     return self.cursor.lastrowid
        # except pymysql.err.IntegrityError as e:
        #     if e.args[0] == 1062:   # Duplicate entry
        #         raise Exception('数据已存在')

    def update(self, sql, data, where, blankword=False):
        '''更新数据
        @param sql (str): SQL语句，格式："UPDATE tab SET & WHERE @"
        @param data (dict): 待更新的数据，例如：{'age':123, 'r.name':'chen'}
        @param where (dict): 限定条件，例如： {'id':[1,2], 'email':'chen@ming'}
        @return: int, 更改的行数

        '''
        assert isinstance(sql, basestring)

        # 替换where
        if where is not None:
            sql = sql.replace('@', '%s')
            sql = sql % self._where_format(where)
            # sql = sql % ('AND'.join([' %s=%s ' % (_kstr_quote(k), _str_quote(where[k])) for k in where.keys()]))

        sql = sql.replace('%', '%%')

        # 替换data
        sql = sql.replace('&', '%s')
        sql = sql % ','.join(['%s=%s' % (
            self._kstr_quote(k),
            self._str_quote(data[k])
        ) for k in data.keys()])
        # 这里将数据的keys与values组合成字符串并拼接到SQL语句中，其中对字符类型（非int/long）
        # 添加了双引号！
        if blankword:
            sql = sql.replace("''", "null")
        logger.debug(sql)

        res = self.cursor.execute(sql)
        self.auto_commit()
        return res

    def delete(self, sql, where=None):
        '''删除数据从数据库
        @param sql: str,insert SQL语句，例如："DELETE FROM tab WHERE %s"
        @param where: dict,限定条件，例如： {'id':11, 'email':'chen@ming'}
        @return: int, 删除的行数

        '''
        assert isinstance(sql, basestring)

        if where is not None:
            sql = sql.replace('@', '%s')
            sql = sql % self._where_format(where)

        # 这里将数据的keys与values组合成字符串并拼接到SQL语句中，其中对字符类型（非int/long）
        # 添加了双引号！
        logger.debug(sql)

        res = self.cursor.execute(sql)
        self.auto_commit()
        return res

    def select(self, items, sql, where=None):
        '''数据库查询.
        @param items: list, 查询列列表，如:
                ['col1','tab.col2','tab.col3$alias']，
                                    其中tab.col2定义了表名，$alias定义列的别名
        @param sql: str,查询的SQL语句，如：
                'select # from tab where id=2',
                                    其中#号将引用由item定义的列表,@用于匹配%swhere条件
        @param where: dict,where条件涉及的列、值字典对象，如:
                where={'a':1, 'b':['chen','ming']}
        @return: result list,查询结果集列表, 每一个值为数据行的key/value字典对象.

        '''
        assert isinstance(sql, basestring)
        assert isinstance(items, list)

        # 替换where
        if where is not None:
            sql = sql.replace('@', '%s')
            sql = sql % self._where_format(where)
            # sql = sql % ('AND'.join([' %s=%s ' % (_kstr_quote(k), _str_quote(where[k])) for k in where.keys()]))

        # 替换列items
        def tmpfun(s, x):
            x = x.split('$')[0]  # 过滤$号及后面的内容
            tmp = x.split('.')  # 对items中的列进行反引号处理，因为sql中需要使用反引号规避关键字
            if len(tmp) == 1:
                return '%s, %s' % (s, self._escape(tmp[0]))
            else:  # length==2
                return '%s, %s.%s' % (s, tmp[0], self._escape(tmp[1]))

        sql = sql.replace('%', '%%')
        sql = sql.replace('#', '%s', 1) % reduce(tmpfun, items, '')[2:]
        logger.debug(sql)

        self.cursor.execute(sql)
        # TODO: check if length of items and query result is equal!
        keys = [i.split('.')[-1].split('$')[-1] for i in items]
        res = [dict(zip(keys, row)) for row in self.cursor.fetchall()]
        if self._fetchencode:  # 对于非unicode字符串需要转换成unicode
            for row in res:
                for k, v in row.items():
                    if isinstance(v, basestring):
                        row[k] = v.decode(self._fetchencode)
        return res

    #######fuzzy_select
    def fuzzy_select(self, items, sql, where=None):
        '''数据库模糊查询.
        @param items: list, 查询列列表，如:
                ['col1','tab.col2','tab.col3$alias']，
                                    其中tab.col2定义了表名，$alias定义列的别名
        @param sql: str,查询的SQL语句，如：
                'select # from tab where id=2',
                                    其中#号将引用由item定义的列列表,@用于匹配%swhere条件
        @param where: dict,where条件涉及的列、值字典对象，如:
                where={'a':1, 'b':['chen','ming']}
        @return: result list,查询结果集列表, 每一个值为数据行的key/value字典对象.
        '''
        assert isinstance(sql, basestring)
        assert isinstance(items, list)

        # 替换where
        if where is not None:
            sql = sql.replace('@', '%s')
            sql = sql % self._where_fuzzyformat(where)
            # sql = sql % ('AND'.join([' %s=%s ' % (_kstr_quote(k), _str_quote(where[k])) for k in where.keys()]))

        # 替换列items
        def tmpfun(s, x):
            x = x.split('$')[0]  # 过滤$号及后面的内容
            tmp = x.split('.')  # 对items中的列进行反引号处理，因为sql中需要使用反引号规避关键字
            print tmp
            if len(tmp) == 1:
                return '%s, %s' % (s, self._escape(tmp[0]))
            else:  # length==2
                return '%s, %s.%s' % (s, tmp[0], self._escape(tmp[1]))

        sql = sql.replace('%', '%%')
        sql = sql.replace('#', '%s', 1) % reduce(tmpfun, items, '')[2:]
        logger.debug(sql)

        self.cursor.execute(sql)
        # TODO: check if length of items and query result is equal!
        keys = [i.split('.')[-1].split('$')[-1] for i in items]
        res = [dict(zip(keys, row)) for row in self.cursor.fetchall()]
        if self._fetchencode:  # 对于非unicode字符串需要转换成unicode
            for row in res:
                for k, v in row.items():
                    if isinstance(v, basestring):
                        row[k] = v.decode(self._fetchencode)
        return res

    #######

    def select_all(self, sql, where=None, set2string=False):
        '''数据库查询，返回所有字段
        @param sql: str,查询的SQL语句，如：
                'select * from tab where id=2',
                                    其中@用于匹配%swhere条件
        @param where: dict,where条件涉及的列、值字典对象，如:
                where={'a':1, 'b':['chen','ming']}
        @param set2string: bool, 是否需要将set结果转换成逗号分隔的字符串,默认不转换
        @return: result list,查询结果集列表, 每一个值为数据                                                                                                                                                                                                                                                                                                                 行的key/value字典对象.

        '''
        assert isinstance(sql, basestring)

        # 替换where
        if where is not None:
            sql = sql.replace('@', '%s')
            sql = sql % self._where_format(where)
            # sql = sql % ('AND'.join([' %s=%s ' % (_kstr_quote(k), _str_quote(where[k])) for k in where.keys()]))

        logger.debug(sql)

        self.cursor.execute(sql)
        keys = [i[0].lower() for i in self.cursor.description]
        if self._fetchencode:  # 对于非unicode字符串需要转换成unicode
            keys = [i.decode(self._fetchencode) for i in keys]

        res = [dict(zip(keys, row)) for row in self.cursor.fetchall()]
        if set2string:
            self._set2string(res)

        if self._fetchencode:  # 对于非unicode字符串需要转换成unicode
            for row in res:
                for k, v in row.items():
                    if isinstance(v, basestring):
                        row[k] = v.decode(self._fetchencode)
        return res

    def select_all_nolog(self, sql, where=None, set2string=False):
        '''数据库查询，返回所有字段
        @param sql: str,查询的SQL语句，如：
                'select * from tab where id=2',
                                    其中@用于匹配%swhere条件
        @param where: dict,where条件涉及的列、值字典对象，如:
                where={'a':1, 'b':['chen','ming']}
        @param set2string: bool, 是否需要将set结果转换成逗号分隔的字符串,默认不转换
        @return: result list,查询结果集列表, 每一个值为数据                                                                                                                                                                                                                                                                                                                 行的key/value字典对象.

        '''
        assert isinstance(sql, basestring)

        # 替换where
        if where is not None:
            sql = sql.replace('@', '%s')
            sql = sql % self._where_format(where)
            # sql = sql % ('AND'.join([' %s=%s ' % (_kstr_quote(k), _str_quote(where[k])) for k in where.keys()]))

        # logger.debug(sql)

        self.cursor.execute(sql)
        keys = [i[0].lower() for i in self.cursor.description]
        if self._fetchencode:  # 对于非unicode字符串需要转换成unicode
            keys = [i.decode(self._fetchencode) for i in keys]

        res = [dict(zip(keys, row)) for row in self.cursor.fetchall()]
        if set2string:
            self._set2string(res)

        if self._fetchencode:  # 对于非unicode字符串需要转换成unicode
            for row in res:
                for k, v in row.items():
                    if isinstance(v, basestring):
                        row[k] = v.decode(self._fetchencode)
        return res

    def select_origin(self, sql, where=None):
        '''数据库查询，返回原始的查询结果
        @param sql: str,查询的SQL语句，如：
                'select * from tab where id=2',
                                    其中@用于匹配%swhere条件
        @param where: dict,where条件涉及的列、值字典对象，如:
                where={'a':1, 'b':['chen','ming']}
        @return: result list,查询结果集列表, list-list

        '''
        assert isinstance(sql, basestring)

        # 替换where
        if where is not None:
            sql = sql.replace('@', '%s')
            sql = sql % self._where_format(where)
            # sql = sql % ('AND'.join([' %s=%s ' % (_kstr_quote(k), _str_quote(where[k])) for k in where.keys()]))

        logger.debug(sql)

        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def execute(self, sql):
        '''执行SQL语句，不建议使用，请尽量使用前面的数据库方法
        @param sql: str，待执行的SQL语句，如: 'select col1,col2 from tab where id=2'
        @return: result list or rows updated/deleted/inserted, 返回结果与执行的SQL有关系

        '''
        assert isinstance(sql, basestring)
        logger.debug(sql)

        return self.cursor.execute(sql)

    def getlastrowid(self):
        '''返回insert的返回ID'''
        return self.cursor.lastrowid

    def _set2string(self, result):
        '''将结果集中的set变量转换成字符串的形式
        @param result: list dict, 结果集
        @return: list dict

        '''
        assert (isinstance(result, list) or isinstance(result, tuple))
        for r in result:
            for k, v in r.items():
                if isinstance(v, set):
                    r[k] = ','.join(v)

    def _where_format_multi(self, where):
        '''
            多表查询
            lhy added
        '''
        ws = []
        for k, v in where.items():
            ws.append('%s=%s' % (self._kstr_quote(k), self._kstr_quote(v)))
        return reduce(lambda m, n: '%s AND %s' % (m, n), ws)

    def select_multi(self, items, sql, where_special, where=None):
        '''
            多表查询
            lhy added
        '''
        assert isinstance(sql, basestring)
        assert isinstance(items, list)

        ssql = self._where_format_multi(where_special)
        if where is not None:
            ssql = ssql + " and " + self._where_format(where)

        sql = sql.replace('@', '%s')
        sql = sql % ssql

        # 替换列items
        def tmpfun(s, x):
            x = x.split('$')[0]  # 过滤$号及后面的内容
            tmp = x.split('.')  # 对items中的列进行反引号处理，因为sql中需要使用反引号规避关键字
            if len(tmp) == 1:
                return '%s, %s' % (s, self._escape(tmp[0]))
            else:  # length==2
                return '%s, %s.%s' % (s, tmp[0], self._escape(tmp[1]))

        sql = sql.replace('%', '%%')
        sql = sql.replace('#', '%s', 1) % reduce(tmpfun, items, '')[2:]
        logger.debug(sql)

        self.cursor.execute(sql)
        # TODO: check if length of items and query result is equal!
        keys = [i.split('.')[-1].split('$')[-1] for i in items]
        res = [dict(zip(keys, row)) for row in self.cursor.fetchall()]
        # print res
        if self._fetchencode:  # 对于非unicode字符串需要转换成unicode
            for row in res:
                for k, v in row.items():
                    if isinstance(v, basestring):
                        row[k] = v.decode(self._fetchencode)
        return res

    def select_multi_nolog(self, items, sql, where_special, where=None):
        '''
            多表查询
            lhy added
        '''
        assert isinstance(sql, basestring)
        assert isinstance(items, list)

        ssql = self._where_format_multi(where_special)
        if where is not None:
            ssql = ssql + " and " + self._where_format(where)

        sql = sql.replace('@', '%s')
        sql = sql % ssql

        # 替换列items
        def tmpfun(s, x):
            x = x.split('$')[0]  # 过滤$号及后面的内容
            tmp = x.split('.')  # 对items中的列进行反引号处理，因为sql中需要使用反引号规避关键字
            if len(tmp) == 1:
                return '%s, %s' % (s, self._escape(tmp[0]))
            else:  # length==2
                return '%s, %s.%s' % (s, tmp[0], self._escape(tmp[1]))

        sql = sql.replace('%', '%%')
        sql = sql.replace('#', '%s', 1) % reduce(tmpfun, items, '')[2:]
        # logger.debug(sql)

        self.cursor.execute(sql)
        # TODO: check if length of items and query result is equal!
        keys = [i.split('.')[-1].split('$')[-1] for i in items]
        res = [dict(zip(keys, row)) for row in self.cursor.fetchall()]
        # print res
        if self._fetchencode:  # 对于非unicode字符串需要转换成unicode
            for row in res:
                for k, v in row.items():
                    if isinstance(v, basestring):
                        row[k] = v.decode(self._fetchencode)
        return res
