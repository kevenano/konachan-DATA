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
import pymysql
import multiprocessing
import copy
import time
import zipfile
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header


# 数据库类
class DB:
    '创建一个mysql数据库对象'
    host = ""
    user = ""
    __passwd = ""
    database = ""
    connection = ""
    cursor = ""

    def __init__(self, host=None, port=None, user=None, passwd="", database=None):
        """
        初始化数据库对象\n
        """
        self.host = host
        self.port = port
        self.user = user
        self.__passwd = passwd
        self.database = database

    def connect(self) -> None:
        """
        连接到数据库\n
        失败直接报错\n
        """
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.__passwd,
                database=self.database,
            )
            self.cursor = self.connection.cursor()
        except Exception as e:
            raise e

    def commit(self):
        self.connection.commit()

    def close(self) -> None:
        """
        关闭数据库连接\n
        """
        self.connection.close()

    def rollback(self):
        self.connection.rollback()

    def execute(self, sql, args=None) -> None:
        """
        执行sql语句\n
        ⚠务必谨慎使用⚠\n
        执行错误直接报错, 同时自动回退\n
        excute之后需要commit\n
        """
        try:
            self.cursor.execute(sql, args)
        except pymysql.err.InterfaceError:
            self.connection.ping(reconnect=True)
            self.cursor.execute(sql, args)
        except pymysql.Error as e:
            self.rollback()
            raise e

    def createTable(
        self,
        tableName,
        columns={"Name": "Type"},
        primaryKey="key",
        engine="innodb",
        de_charset="utf8mb4",
    ) -> None:
        """
        创建表\n
        失败直接报错\n
        无需额外commit\n
        """
        sql = """create table `""" + tableName + """`("""
        for clName, clType in columns.items():
            sql = sql + "`" + clName + "`" + " " + clType + ","
        sql = sql + "primary key" + "(`" + primaryKey + "`)" + ")"
        sql = sql + "engine=" + engine + " " + "default charset=" + de_charset
        try:
            self.execute(sql)
            self.commit()
        except Exception as e:
            raise e

    def dropTable(self, tablesName) -> None:
        """
        删除表\n
        失败直接报错\n
        无需额外commit\n
        """
        sql = """drop table """
        for item in tablesName:
            sql = sql + "`" + item + "`" + ","
        sql = sql[0: len(sql) - 1]
        try:
            self.execute(sql)
            self.commit()
        except Exception as e:
            raise e

    def insert(self, tableName, fileds, values) -> None:
        """
        插入数据\n
        失败直接报错\n
        无需额外commit\n
        """
        fileds = str(tuple(fileds)).replace("""'""", "`")
        values = str(tuple(values))
        tableName = "`" + tableName + "`"
        sql = """insert into """ + tableName + " "
        sql = sql + fileds + " values " + values
        try:
            self.execute(sql)
            self.commit()
        except Exception as e:
            raise e

    def replace(self, tableName, fileds, values) -> None:
        """
        替换插入\n
        失败直接报错\n
        无需额外commit\n
        """
        fileds = str(tuple(fileds)).replace("""'""", "`")
        values = str(tuple(values))
        tableName = "`" + tableName + "`"
        sql = """REPLACE INTO """ + tableName + " "
        sql = sql + fileds + " values " + values
        try:
            self.execute(sql)
            self.commit()
        except Exception as e:
            raise e

    def fetchall(self):
        results = self.cursor.fetchall()
        return results

    def dump(self, bkDir: str):
        '''
        备份\n
        使用bz2算法创建压缩文件\n
        输入存放备份文件的目录\n
        成功返回压缩备份文件路径\n
        失败直接报错\n
        '''
        if not os.path.exists(bkDir):
            os.makedirs(bkDir)
        fileName = time.strftime('%Y%m%d-%H%M%S')
        sqlPath = os.path.join(bkDir, fileName + '.sql')
        cmpPath = os.path.join(bkDir, fileName + '.zip')
        # dumpcmd = f"mysqldump -h{self.host} -u{self.user} -p{self.__passwd} {self.database} > {sqlPath} > /dev/null 2>&1"
        dumpcmd = f"mysqldump -h{self.host} -u{self.user} -p{self.__passwd} {self.database} > {sqlPath}"
        res = os.system(dumpcmd)
        if res == 0:
            try:
                with zipfile.ZipFile(cmpPath, 'w') as z:
                    z.write(filename=sqlPath, arcname=os.path.basename(
                        sqlPath), compress_type=zipfile.ZIP_BZIP2, compresslevel=1)
                os.remove(sqlPath)
                return cmpPath
            except Exception as e:
                raise e
        else:
            raise 'Fail to make backup!'

    def restore(self, zipPath: str) -> bool:
        '''
        还原\n
        输入.zip文件的路径\n
        成功返回True\n
        失败直接报错\n
        '''
        try:
            with zipfile.ZipFile(zipPath, 'r') as z:
                sqlPath = os.path.join(os.path.dirname(zipPath), z.namelist()[0])
                z.extractall(path=os.path.dirname(zipPath))
        except Exception as e:
            raise e
        # restcmd = f"mysql -h{self.host} -u{self.user} -p{self.__passwd} {self.database} < {sqlPath} > /dev/null 2>&1"
        restcmd = f"mysql -h{self.host} -u{self.user} -p{self.__passwd} {self.database} < {sqlPath}"
        res = os.system(restcmd)
        os.remove(sqlPath)
        if res == 0:
            return True
        else:
            raise 'Fail to restore database!'


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


class QQMail:
    """
    QQ邮箱SMTP客户端\n
    """
    # 创建一个带附件的实例
    message = MIMEMultipart()

    def __init__(self, sender: str, passwd: str, receivers: list):
        """
        初始化\n
        """
        self.mail_host = "smtp.qq.com"
        self.sender = sender
        self.mail_pass = passwd
        self.receivers = receivers

    def creatMessage(self):
        """创建邮件"""
        # self.message['From'] = Header("菜鸟教程", 'utf-8')
        # self.message['To'] =  Header("测试", 'utf-8')
        # subject = 'Python SMTP 邮件测试'
        # self.message['Subject'] = Header(subject, 'utf-8')

        # #邮件正文内容
        # self.message.attach(MIMEText('这是菜鸟教程Python 邮件发送测试……', 'plain', 'utf-8'))

        # # 构造附件1，传送当前目录下的 test.txt 文件
        # att1 = MIMEText(open('test.txt', 'rb').read(), 'base64', 'utf-8')
        # att1["Content-Type"] = 'application/octet-stream'
        # # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        # att1["Content-Disposition"] = 'attachment; filename="test.txt"'
        # self.message.attach(att1)

        # # 构造附件2，传送当前目录下的 runoob.txt 文件
        # att2 = MIMEText(open('runoob.txt', 'rb').read(), 'base64', 'utf-8')
        # att2["Content-Type"] = 'application/octet-stream'
        # att2["Content-Disposition"] = 'attachment; filename="runoob.txt"'
        # self.message.attach(att2)
    
    def addAtt(self,filePath:str,name:str):
        """添加文件到附件"""
        with open(filePath,'rb') as f:
            att = MIMEApplication(f.read())
        att.add_header('Content-Disposition', 'attachment', filename=name)
        self.message.attach(att)
    
    def send(self):
        """发送邮件"""
        self.creatMessage()
        smtpObj = smtplib.SMTP_SSL(self.mail_host, 465)
        smtpObj.login(self.sender, self.mail_pass)
        smtpObj.sendmail(self.sender, self.receivers, self.message.as_string())
        smtpObj.quit()


def test():
    mp = multiProTask(1)
    print(mp.processNum)
    print(multiProTask.processNum)


if __name__ == "__main__":
    test()
