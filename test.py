# coding: utf-8
import MySQLdb
import os
from asynctorndb import converters

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

db_config = {
    "host": "localhost", # 地址
    "user": "root", # 用户
    "passwd": "", # 密码
    "db": "ull_community", # 数据库
}

connect = MySQLdb.connect(**db_config)
# 得到游标
cursor = connect.cursor()


def _escape_args(args, charset):
    if isinstance(args, (tuple, list)):
        return tuple(converters.escape_item(arg, charset) for arg in args)
    elif isinstance(args, dict):
        return dict((key, converters.escape_item(val, charset)) for (key, val) in list(args.items()))
    else:
        # If it's not a dictionary let's try escaping it anyways.
        # Worst case it will throw a Value error
        return converters.escape_item(args, charset)


class CustomException(Exception):
    pass


class Entity(object):
    def __init__(self):
        self.table = "entity"
        self.charset = "utf-8"
        self.sql = ""
        self.data_format = ""
        self.operator_symbol = {
            "is": " is ",
            "is_not": " is not ",
            "in": " in ",
            "is_not_in": "is not in ",
            "lt": "<",
            "gt": ">",
            "eq": "=",
            "gte": ">=",
            "lte": "<=",
            "neq": "!=",
        }

    def _select_sql(self):
        """
        公共部分SELECT
        :return:
        """
        sql = "SELECT %s FROM %s"
        fields = ",".join(self.fields)
        sql = sql % (fields, self.table)
        return sql

    def _where_sql(self, **kwargs):
        """
        组织where条件部分
        :return:
        """
        # field symbol value and_or
        # 解析kwargs, where field = %s %s
        where = " WHERE 1=1 AND "
        for key, val in kwargs.items():
            # val = [field_value, AND_OR]
            field, symbol_key = key.split("__")

            symbol = self.operator_symbol.get(symbol_key)
            if not symbol:
                raise CustomException("not found symbol: {}".format(symbol_key))
            # TODO: 判断对应操作符对应值
            _val, and_or = val
            where += "%s%s" % (field, symbol)
            where += converters.escape_item(_val, self.charset)
            where += " %s" % and_or
        return where

    def _filter_sql(self, **kwargs):
        """
        过滤完整sql
        :param kwargs:
        :return:
        """
        sql = self._select_sql()
        where = self._where_sql(**kwargs)

        sql += where
        return sql

    def result(self, **kwargs):
        """
        得到查询结果
        :param kwargs:
        :return:
        """
        print self.sql

        cursor.execute(self.sql)
        dataset = cursor.fetchall()

        print dataset
        return dataset

    def count(self):
        """
        总数量
        :return:
        """
        # 得到查询列前一个空格索引
        first_space = self.sql.find(" ", 0)
        # 截取到FROM前一个空格索引
        second_space = self.sql.find(" ", first_space + 1)
        select_sql = "SELECT count(id) "

        sql = select_sql + self.sql[second_space:]
        self.sql = sql
        return self

    def sum(self):
        """
        求和
        :return:
        """

    def limit(self, start, page_size):
        """
        分页
        :return:
        """
        sql = self.sql + " limit %s, %s" % (start, page_size)
        self.sql = sql
        return self

    def order_by(self, field, sort_style):
        """
        排序
        :param field: 排序的列(字符串, 列表, 元组)
        :param sort_style: 排序方式(asc, desc)
        :return:
        """
        fields = field
        if isinstance(field, (list, tuple)):
            fields = ",".join(field)

        sql = self.sql + " ORDER BY %s %s" % (fields, sort_style)
        self.sql = sql
        return self

    def all(self):
        """
        返回当前对象
        如需得到结果条用result方法
        :return:
        """
        self.sql = self._select_sql()
        return self

    def filters(self, **kwargs):
        """
        :param kwargs:
        :return:
        """
        self.sql = self._filter_sql(**kwargs)
        return self

    def insert(self):
        pass

    def insert_many(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass


class Pro(Entity):
    def __init__(self):
        super(Pro, self).__init__()
        self.fields = ["id", "name"]
        self.table = "user_info"

p = Pro()

p.all()
p.filters(status__eq=[None, ""]).order_by("id", "asc").limit(10, 10).count().result()

