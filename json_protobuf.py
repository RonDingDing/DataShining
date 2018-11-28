import sys
import db_conf
import datetime

sys.path.append('../svc_base')
sys.path.append('../svc_common')
import json
from redisdb import RedisMan
from mydb import PymysqlMan
import datetime

sqlconn = PymysqlMan()
dbconn = RedisMan(db_conf.REDIS_DB_HOST,
                  db_conf.REDIS_DB_PORT,
                  password=db_conf.REDIS_DB_PASSWORD,
                  db=db_conf.REDIS_DB_NAME)


class Data(object):
    def __init__(self, table_name, code=None, expire_time=86400, delimiter='--------'):

        self.table_name = table_name
        self.code = code
        self.prefix = self.table_name.replace('tb_', '')
        self.keys = []
        self.expire_time = expire_time
        self.delimiter = str(delimiter)


    def _search_sql(self, condition, keys="*"):

        if isinstance(keys, (list, tuple)):
            keys = ', '.join(keys)
        else:
            keys = str(keys)
        if not condition:
            condition = {}
        dict_condition = dict(condition)
        sql = "SELECT {} FROM {} WHERE ".format(keys, self.table_name)
        param = []
        for k, v in dict_condition.items():
            sql += "`{}`= %s AND ".format(k)
            param.append(v)
        sql = sql.rstrip("AND ") + ';'
        return sql, param

    def _insert_sql(self, dic, condition=None):
        if not condition:
            condition = {}
        sql1 = 'INSERT INTO {} ('.format(self.table_name)
        sql2 = 'VALUES ('
        param2 = []
        condition = dict(condition)
        condition.update(dic)

        for k, v in condition.items():
            sql1 += '`{}`, '
            sql1 = sql1.format(k)
            sql2 += '%s, '
            param2.append(v)
        sql1 = sql1.rstrip(' ').rstrip(',')
        sql2 = sql2.rstrip(' ').rstrip(',')
        sql1 += ') '
        sql2 += ') '

        sql = sql1 + sql2 + ';'
        param = param2
        return sql, param

    def _update_sql(self, dic, condition=None):
        sql1 = "UPDATE {} SET ".format(self.table_name)
        sql2 = "WHERE "
        param1 = []
        param2 = []
        if not condition:
            condition = {}
        condition = dict(condition)
        real_dic = condition.copy()
        real_dic.update(dic)
        dic = real_dic

        for k, v in dic.items():
            sql1 += '`{}`=%s, '.format(k)
            param1.append(v)
        sql1 = sql1.rstrip(' ').rstrip(',') + ' '

        for key, value in condition.items():
            sql2 += ' `{}`=%s AND'.format(key)
            param2.append(value)
        sql = (sql1 + sql2).rstrip('AND') + ';'
        param = param1 + param2

        return sql, param

    def _load_all_from_mysql(self, condition=None):
        sql, param = self._search_sql(condition)
        result = []
        raw_result = sqlconn.queryAll(sql, param)
        if raw_result:
            for i in raw_result:
                dic = {}
                for k, v in i.items():
                    if isinstance(v, (datetime.timedelta, datetime.time, datetime.date, datetime.datetime)):
                        dic[k] = str(v)
                    else:
                        dic[k] = v
                result.append(dic)

        return result

    def _load_one_from_mysql(self, condition=None):
        sql, param = self._search_sql(condition)
        raw_result = sqlconn.queryOne(sql, param)
        result = {}
        if raw_result:
            for k, v in raw_result.items():
                if isinstance(v, (datetime.timedelta, datetime.time, datetime.date, datetime.datetime)):
                    result[k] = str(v)
                else:
                    result[k] = v
        return result

    def insert_one(self, dic):
        sql, param = self._insert_sql(dic)
        sqlconn.execute_immediately(sql, param)
        self.clear_redis()
        self.load_all()

    def update_one(self, dic, condition=None):
        sql, param = self._update_sql(dic, condition)
        sqlconn.execute_immediately(sql, param)
        self.clear_redis()
        self.load_all()

    def insert_or_update(self, dic, condition=None):
        data = self._load_one_from_mysql(condition)
        if data:
            self.update_one(dic, condition)
        else:
            dic.update(dict(condition))
            self.insert_one(dic)

    def clear_redis(self):
        """
        API: 清除本表的Redis记录
        :return:
        """
        keys = ''.join([str(self.prefix), ':*'])
        all_redis_keys = [str(o, 'utf-8') for o in dbconn.conn().keys(keys)]
        for i in all_redis_keys:
            dbconn.conn().delete(i)

    def load_result_to_redis(self, condition):
        """
        API: 把 mysql 的查询结果存到 redis
        """
        raw_result = self._load_all_from_mysql(condition)
        num = 0
        for each_result in raw_result:
            num += 1
            for k, v in each_result.items():
                root_key = ''.join([str(self.prefix), ':', str(num)])
                if v is None:
                    typing = 'None'
                elif isinstance(v, int):
                    typing = 'int'
                elif isinstance(v, float):
                    typing = 'float'
                elif isinstance(v, datetime.datetime):
                    typing = 'datetime'
                elif isinstance(v, datetime.date):
                    typing = 'date'
                elif isinstance(v, datetime.date):
                    typing = 'time'
                else:
                    typing = 'str'
                dbconn.conn().hset(root_key, str(k), ''.join([str(v), self.delimiter, typing]))
                dbconn.conn().expire(root_key, self.expire_time)
        return raw_result

    def _load_all_from_redis(self, condition=None):
        if not condition:
            condition = {}
        condition = dict(condition)
        keys = ''.join([str(self.prefix), ':*'])
        all_redis_keys = [str(o, 'utf-8') for o in dbconn.conn().keys(keys)]
        return_list = []
        for i in all_redis_keys:
            one_result = {str(k, 'utf-8'): str(v, 'utf-8') for k, v in dbconn.conn().hgetall(i).items()}

            find = True
            for kkkk, vvv in condition.items():
                if one_result.get(str(kkkk), '').split(self.delimiter)[0] != str(vvv):
                    find = False
            if find:
                dic = {}
                for k, v in one_result.items():
                    value, typing = v.split(self.delimiter)
                    if typing == 'None':
                        dic[k] = None
                    elif typing == 'int':
                        dic[k] = int(value)
                    elif typing == 'float':
                        dic[k] = float(value)
                    else:
                        dic[k] = str(value)
                return_list.append(dic)
        return return_list

    def _load_one_from_redis(self, condition=None):
        all_raw_result = self._load_all_from_redis(condition)
        return all_raw_result[0] if all_raw_result else {}

    def load_one(self, condition=None):
        """
        API: 读取一个符合条件的数据
        :return:
        """
        redis_result = self._load_one_from_redis(condition)
        if redis_result:
            return redis_result
        self.clear_redis()
        mysql_result = self.load_result_to_redis(condition)
        if mysql_result:
            return mysql_result[0]
        return {}

    def load_all(self, condition=None):
        """
        API: 读取所有符合条件的数据
        :return:
        """
        redis_result = self._load_all_from_redis(condition)
        if redis_result:
            return redis_result
        self.clear_redis()
        mysql_result = self.load_result_to_redis(condition)
        if mysql_result:
            return mysql_result
        return []


if __name__ == "__main__":
    a = Data('test')
