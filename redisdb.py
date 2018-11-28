#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""db.py.

数据接口

"""
import redis
import logging




class RedisMan(object):
    """数据库接口
    """
    """数据库实例对象"""
    instance = None


    @staticmethod
    def instantiate(host, port, sock, password, db):
        """获得数据库对象实例
        """
        if RedisMan.instance is None:
            RedisMan.instance = RedisMan(host, port,  password, db, sock)
        return RedisMan.instance

    def __init__(self, host, port, password, db, sock=None):
        """初始化
        """
        logging.info("connecting to DB[host:%s port:%d socket:%s db:%d]..." % (host, port, sock, db))
        self.host = host
        self.port = port
        self.sock = sock
        self.password = password
        self.db = db
        if sock:
            self.dbconn = redis.Redis(unix_socket_path=self.sock,
                                       host=self.host,
                                       port=self.port,
                                       password=self.password,
                                       db=self.db)
        else:
            self.dbconn = redis.Redis(
                                       host=self.host,
                                       port=self.port,
                                       password=self.password,
                                       db=self.db)

        self.ping()
        # 流水线
        self.pipe = self.dbconn.pipeline()

    def ping(self):
        """定时ping
        """
        self.dbconn.ping()


    def conn(self):
        return self.dbconn

    def pipe(self):
        return self.pipe

