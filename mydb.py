#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import pymysql
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
import time
import random
import logging
import json
import db_conf



class PymysqlPool(object):
    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Mysql.getConn()
    释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self):
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = self.__getConn()
        self._cursor = self._conn.cursor()

    def __getConn(self):
        """
        @summary: 从连接池中取出连接
        @return MySQLdb.connection
        """
        if PymysqlPool.__pool is None:
            PymysqlPool.__pool = PooledDB(creator=pymysql,
                                          mincached=db_conf.MYSQL_MINCACHED,
                                          maxcached=db_conf.MYSQL_MAXCACHED,
                                          host=db_conf.MYSQL_DB_HOST,
                                          port=db_conf.MYSQL_DB_PORT,
                                          user=db_conf.MYSQL_DB_USER,
                                          passwd=db_conf.MYSQL_DB_PASS,
                                          db=db_conf.MYSQL_DB_NAME,
                                          use_unicode=False,
                                          charset="utf8",
                                          cursorclass=DictCursor)
        return PymysqlPool.__pool.connection()

    def query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        return count

    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list 查询到的结果集
        """
        count = self.query(sql, param)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = []
        return result

    def getOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result dict 查询到的结果集
        """
        count = self.query(sql, param)
        if count > 0:
            result = self._cursor.fetchone()
        else:
            result = {}
        return result

    def getMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list 查询到的结果集
        """
        count = self.query(sql, param)
        if count > 0:
            result = self._cursor.fetchmany(num)
        else:
            result = []
        return result

    def insertMany(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = self._cursor.executemany(sql, values)
        return count

    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    def insert(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)

    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self, isEnd=1):
        """
        @summary: 释放连接池资源
        """
        if isEnd == 1:
            self.end('commit')
        else:
            self.end('rollback')
        self._cursor.close()
        self._conn.close()


class PymysqlMan(object):
    def __init__(self):
        self.timer = None

    def queryOne(self, sql, param=None):
        pool = PymysqlPool()
        result = pool.getOne(sql, param)
        pool.dispose()

        for key, value in result.items():
            if isinstance(value, bytes):
                result[key] = value.decode('utf-8')

        return result

    def queryAll(self, sql, param=None):
        pool = PymysqlPool()
        result = pool.getAll(sql, param)
        pool.dispose()
        if result:
            for item in result:
                for key, value in item.items():
                    if isinstance(value, bytes):
                        item[key] = value.decode('utf-8')
        else:
            result = []
        return result

    def execute_immediately(self, sql, param=None):
        """立刻执行
        """
        try:
            pool = PymysqlPool()
            pool.query(sql, param)
            pool.dispose()
            result = True
        except:

            result = False
        return result



if __name__ == '__main__':
    sqldb = PymysqlMan()

    # param = ['123456', '123457']
    # sql = "select username from tb_user where username=%s or username=%s"
    # result = sqldb.queryAll(sql, param)
    # print(result)

    bag_infos = {}
    param = []
    sql = "select * from tb_more_game"
    # print(sql, param)
    logging.info(sql % tuple(param))
    logging.info("%s-%r", sql, param)
    results = sqldb.queryOne(sql, param)
    #print(results)
    # for item in results:
    #     itemid = str(item['itemid'])
    #     bag_infos[itemid] = item
    # #print(bag_infos)

    #
    # from userinfo import get_bag_infos
    # print(get_bag_infos(2, 1007650, 2))

    #
    # t1 = time.time()
    #
    # sqlAll = "select * from tb_user where uid=%s;"
    # uid = random.randint(1000000, 9999999)
    # param = [str(uid)]
    # result = pool.getAll(sqlAll, param)
    # print(result)
    # t2 = time.time()
    # print('times:', t2-t1)

    # 释放资源
    # pool.dispose()
