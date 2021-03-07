#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   CLASS.py
@Desc    :   常用class集合
@Version :   2.0
@Time    :   2021/02/18 17:53:53
@Author  :   kevenano 
@Contact :   kevenano@outloook.com
'''

# Hear put the import lib
import os
from pathlib import Path
import pymysql
import multiprocessing
import copy
import time


# 数据库类
class DB:
    '创建一个mysql数据库对象'
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
            host=self.host, user=self.user, password=self.__passwd, database=self.database)
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
    
    def dump(self, bkDir: Path) -> Path:
        '''
        备份\n
        输入存放备份文件的目录\n
        成功返回备份文件路径\n
        失败返回None\n
        '''
        if not os.path.exists(bkDir):
            os.makedirs(bkDir)
        fileName = time.strftime('%Y%m%d-%H%M%S') + '.sql'
        bkPath = os.path.join(bkDir, fileName)
        dumpcmd = f"mysqldump -h{self.host} -u{self.user} -p{self.__passwd} {self.database} > {bkPath}"
        res = os.system(dumpcmd)
        if res == 0:
            return bkPath
        else:
            return None
    
    def restore(self, bkPath: Path) -> bool:
        '''
        还原\n
        输入.sql文件的路径\n
        成功返回True
        失败返回False
        '''
        restcmd = f"mysql -h {self.host} -u {self.user} -p {self.__passwd} {self.database} < {bkPath}"
        res = os.system(restcmd)
        if res == 0:
            return True
        else:
            return False




# 多进程任务
class multiProTask:
    """
    创建一个多进程任务对象\n
    多个进程之间可使用\n
    公共list: AList, BList, CList, failList\n
    公共dict: shareDict\n
    注意:\n
    使用shareDict时，应当在singlePro中初始化shareDict,\n
    即先判断len(shareDict)是否为0，若为0，则表明该字典时初次使用，应当初始化.\n
    同时，单个进程中修改shareDict时，要小心，否则可能修改失败.\n
    如，shareDict['key'] = []
    shareDict['key'].append(v00) -> 修改失败!
    shareDict['key'] = shareDict['key']+[v00] -> 修改成功!
    """
    projectList = []
    taskList = []
    failList = []
    AList = []
    BList = []
    CList = []
    shareDict = {}
    startTime = 0
    endTime = 0
    processNum = multiprocessing.cpu_count()
    processLock = multiprocessing.Lock()

    def __init__(self, projectList: list, processNum: int = None) -> None:
        """
        初始化\n
        确定进程数目\n
        为各进程分配任务\n
        """
        self.projectList = projectList
        if processNum:
            # if processNum > 0 and processNum <= multiProTask.processNum:
            if processNum > 0:
                self.processNum = processNum
        if self.processNum > len(self.projectList):
            self.processNum = len(self.projectList)
        for i in range(self.processNum):
            self.taskList.append(self.projectList[i::self.processNum])

    def singlePro(self, task) -> None:
        '单进程任务'
        for item in task:
            try:
                pass
            except Exception as e:
                self.failList.append(item)

    def multiPro(self) -> None:
        '多进程任务'
        proList = []
        proID = 0
        # 目标函数参数设置
        kwargs = {}
        for task in self.taskList:
            kwargs["task"] = task
            taskProcess = multiprocessing.Process(
                target=self.singlePro, kwargs=kwargs)
            proList.append(taskProcess)
            taskProcess.start()
            proID += 1

        # 等待进程结束
        for process in proList:
            process.join()

    def run(self) -> None:
        '启动该多进程任务'
        with multiprocessing.Manager() as proManager:
            self.startTime = time.time()

            self.failList = proManager.list([])
            self.AList = proManager.list([])
            self.BList = proManager.list([])
            self.CList = proManager.list([])
            self.shareDict = proManager.dict({})

            self.multiPro()

            failList = copy.copy(self.failList[:])
            AList = copy.copy(self.AList[:])
            BList = copy.copy(self.BList[:])
            CList = copy.copy(self.CList[:])
            shareDict = {}
            for k, v in self.shareDict.items():
                shareDict[k] = v

            self.failList = copy.copy(failList)
            self.AList = copy.copy(AList)
            self.BList = copy.copy(BList)
            self.CList = copy.copy(CList)
            self.shareDict = shareDict

            del(failList)
            del(AList)
            del(BList)
            del(CList)
            del(shareDict)

            self.afterDeal()

            self.endTime = time.time()

    def afterDeal(self) -> None:
        '追加任务'
        pass


def test():
    mp = multiProTask(1)
    print(mp.processNum)
    print(multiProTask.processNum)


if __name__ == "__main__":
    test()
