"""
mysql 数据库类
"""
import pymysql


# 数据库类
class DB:
    host = ""
    user = ""
    __passwd = ""
    database = ""
    connection = ""
    cursor = ""

    def __init__(self, host=None, user=None, passwd="", database=None):
        self.host = host
        self.user = user
        self.__passwd = passwd
        self.database = database

    def connect(self):
        self.connection = pymysql.connect(
            self.host, self.user, self.__passwd, self.database
        )
        self.cursor = self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()

    def rollback(self):
        self.connection.rollback()

    def execute(self, sql, args=None):
        try:
            self.cursor.execute(sql, args)
            return 1
        except pymysql.err.InterfaceError:
            self.connection.ping(reconnect=True)
            self.cursor.execute(sql, args)
            return 1
        except pymysql.Error as e:
            # print(e.args[0])
            self.rollback()
            return e

    def createTable(
        self,
        tableName,
        columns={"Name": "Type"},
        primaryKey="key",
        engine="innodb",
        de_charset="utf8mb4",
    ):
        sql = """create table `""" + tableName + """`("""
        for clName, clType in columns.items():
            sql = sql + "`" + clName + "`" + " " + clType + ","
        sql = sql + "primary key" + "(`" + primaryKey + "`)" + ")"
        sql = sql + "engine=" + engine + " " + "default charset=" + de_charset
        flag = self.execute(sql)
        self.commit()
        return flag

    def dropTable(self, tablesName):
        sql = """drop table """
        for item in tablesName:
            sql = sql + "`" + item + "`" + ","
        sql = sql[0: len(sql) - 1]
        flag = self.execute(sql)
        self.commit()
        return flag

    def insert(self, tableName, fileds, values):
        fileds = str(tuple(fileds)).replace("""'""", "`")
        values = str(tuple(values))
        tableName = "`" + tableName + "`"
        sql = """insert into """ + tableName + " "
        sql = sql + fileds + " values " + values
        flag = self.execute(sql)
        self.commit()
        return flag

    def replace(self, tableName, fileds, values):
        """替换插入"""
        fileds = str(tuple(fileds)).replace("""'""", "`")
        values = str(tuple(values))
        tableName = "`" + tableName + "`"
        sql = """REPLACE INTO """ + tableName + " "
        sql = sql + fileds + " values " + values
        flag = self.execute(sql)
        self.commit()
        return flag

    def fetchall(self):
        results = self.cursor.fetchall()
        return results

    def update(self, tableName, filed, value, whereClause):
        sql = "update " + "`" + tableName + "`" + " "
        sql = sql + "set " + "`" + filed + "`" + "=%s "
        sql = sql + whereClause
        flag = self.execute(sql, value)
        self.commit()
        return flag
