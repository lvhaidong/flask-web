# -*- coding: utf-8 -*-
from tool import pysql


def get_rp_pool():
    db = pysql.CURSOR(dbid='PGSQL_CLOUD')
    sql = ' select * from student; '
    return db.select_all(sql=sql)


if __name__ == '__main__':
    get_rp_pool()
