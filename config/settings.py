# -*- coding: utf-8 -*-

# ----------------------------
# 根据setting.py文件所在目录名称来
# 自动判断程序部署状态，是生产还是测试
# ----------------------------
import os

mode = os.environ.get('MODE')
# if mode == 'TESTING':
#     DEBUG = True
# elif mode == 'PRODUCTION':
#     DEBUG = False
# else:
#     raise Exception('Logical error')

#########################################
# 数据库配置
# 要求必须符合格式：MYSQL|ORACLE_数据库名称_参数名=value
#########################################
# MYSQL数据库
MYSQL_MAIN_HOST = '127.0.0.1'
MYSQL_MAIN_PORT = 3306
MYSQL_MAIN_USER = 'root'
MYSQL_MAIN_PASSWD = 'mysql'
MYSQL_MAIN_DB = 'test'
MYSQL_MAIN_CHARSET = 'utf8'

#########################################
# 数据库配置
# 要求必须符合格式：MYSQL|ORACLE|PGSQL_数据库名称_参数名=value
# 需要注意：配置项KEY必须大写！
#########################################
# manageiq 虚拟化平台配置数据库
PGSQL_CLOUD_HOST = '127.0.0.1'
PGSQL_CLOUD_PORT = 5432
PGSQL_CLOUD_USER = 'lvhaidong'
PGSQL_CLOUD_PASSWD = '123456'
PGSQL_CLOUD_DB = 'test'


