#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   insertData.py
@Desc    :   写数据库 json to mysql
@Version :   2.0
@Time    :   2021/02/18 21:59:09
@Author  :   kevenano 
@Contact :   kevenano@outloook.com
'''

# Hear put the import lib
from pathlib import Path
import json
import os
import sys
from pprint import pformat
from CLASS import DB, multiProTask
import argparse


# 面向对象方法
class mpTask(multiProTask):
    typeDic = {
        "id": "int",
        "tags": "text",
        "created_at": "int",
        "creator_id": "int",
        "author": "text",
        "change": "int",
        "source": "text",
        "score": "int",
        "md5": "text",
        "file_size": "int",
        "file_url": "text",
        "is_shown_in_index": "text",
        "preview_url": "text",
        "preview_width": "int",
        "preview_height": "int",
        "actual_preview_width": "int",
        "actual_preview_height": "int",
        "sample_url": "text",
        "sample_width": "int",
        "sample_height": "int",
        "sample_file_size": "int",
        "jpeg_url": "text",
        "jpeg_width": "int",
        "jpeg_height": "int",
        "jpeg_file_size": "int",
        "rating": "text",
        "has_children": "text",
        "parent_id": "int",
        "status": "text",
        "width": "int",
        "height": "int",
        "is_held": "text",
        "frames_pending_string": "text",
        "frames_pending": "text",
        "frames_string": "text",
        "frames": "text",
        "flag_detail": "text",
    }
    tableName = ''

    def __init__(self, tableName: str, projectList: list, processNum: int = None) -> None:
        """
        初始化\n
        给定数据库表名\n
        给定projectList\n
        给定进程数\n
        """
        super().__init__(projectList, processNum=processNum)
        self.tableName = tableName
        # 测试数据库连接
        db = DB("localhost", "root", "qo4hr[Pxm7W5", "konachan")
        db.connect()
        # 建表
        flag = db.createTable(tableName, columns=self.typeDic, primaryKey="id")
        db.close()
        if flag == 1:
            print("建表成功!")
        elif flag.args[0] == 1050:
            print("表格已存在!")
        else:
            print("建表失败!")
            print(flag.args[0], flag.args[1])
            exit()

    def singlePro(self, task: list) -> None:
        """
        单进程任务
        """
        db = DB("localhost", "root", "qo4hr[Pxm7W5", "konachan")
        db.connect()
        db.execute(sql='''SET FOREIGN_KEY_CHECKS = 0 ''')
        for item in task:
            try:
                jsFile = open(item, "r", encoding="utf-8")
                data = json.load(jsFile)
                jsFile.close()
                self.insertData(db, data)
                if self.processLock.acquire():
                    print(str(item),' Done!')
                    print()
                    self.processLock.release()
            except Exception as e:
                db.execute(sql='''SET FOREIGN_KEY_CHECKS = 1 ''')
                db.close()
                self.failList.append(item)
                if self.processLock.acquire():
                    print("####################################")
                    print(str(item)," fail!")
                    print("####################################")
                    print()
                    self.processLock.release()
        db.execute(sql='''SET FOREIGN_KEY_CHECKS = 1 ''')
        db.close()

    def insertData(self, db, data):
        """
        更新数据库\n
        replace方法\n
        data必须为字典列表(直接由json.load转换得来)\n
        """
        for work in data:
            for k, v in work.items():
                if self.typeDic[k] in ["text", "char(10)"]:
                    work[k] = str(v)
                if self.typeDic[k] == "int" and type(v) is not int:
                    work[k] = -1
            flag = db.replace(self.tableName, tuple(work.keys()), tuple(work.values()))
            if flag != 1:
                """更新失败"""
                raise Exception("Faild to insert data!")
                # self.failList.append(work["id"])
                # break
    
    def updateData(self, db, data):
        """
        不建议使用
        建议使用insertData替换
        更新数据库\n
        update方法\n
        data必须为字典列表(直接由json.load转换得来)\n
        """
        for work in data:
            for k, v in work.items():
                if self.typeDic[k] in ["text", "char(10)"]:
                    work[k] = str(v)
                if self.typeDic[k] == "int" and type(v) is not int:
                    work[k] = -1
                if k == "id":
                    continue
                flag = db.update(
                    self.tableName, k, work[k], r"where `id`=" + str(work["id"]))
                if flag != 1:
                    self.failList.append(work["id"])
                    break


# 获取参数
def getParam():
    """
    获取参数\n
    jsonList\n
    jsonFolder\n
    tableName\n
    """
    parser = argparse.ArgumentParser(
        description="konachan data insert tool, json to mysql.")
    
    parGroup0 = parser.add_mutually_exclusive_group(required=True)
    parGroup0.add_argument("-jl", "--jsonList",
                           action="extend",
                           nargs="+",
                           type=Path,
                           dest="jsonList",
                           help="List of json file.",
                           metavar="XXX.json")
    parGroup0.add_argument("-jf", "--jsonFolder",
                           action="store",
                           nargs=1,
                           type=Path,
                           dest="jsonFolder",
                           help="Folder that contain  all json file.",
                           metavar=r"XXX\XXX\XXX")
    
    parser.add_argument("-tn", "--tableName",
                           action="store",
                           nargs=1,
                           type=str,
                           dest="tableName",
                           help="Mysql tableName.",
                           metavar=r"XXX",
                           required=True)

    parser.add_argument("-pn", "--processNum",
                           action="store",
                           nargs=1,
                           type=int,
                           dest="processNum",
                           help="Number of process.",
                           required=False)
    
    # args = parser.parse_args(["-jf",r"E:\konachan\log\1613565477\json","-tn","main"])
    args = parser.parse_args()
    return args


# 主函数
def main():
    # 获取参数
    argvs = getParam()

    # 获取json文件列表
    jsList = []
    if argvs.jsonFolder:
        for folderName, subfolders, fileNames in os.walk(argvs.jsonFolder[0]):
            for fileName in fileNames:
                if fileName.endswith(".json"):
                    jsList.append(os.path.join(folderName, fileName))
    elif argvs.jsonList:
        for item in argvs.jsonList:
            jsList.append(item)

    # 创建多进程对象
    newMPT = mpTask(tableName=argvs.tableName[0], projectList=jsList, processNum=argvs.processNum[0])
    newMPT.run()

    # 写日志
    sumPath = os.path.join(os.getcwd(), str(newMPT.endTime) + ".log")
    sumFile = open(sumPath, "w", encoding="utf-8")
    for item in sys.argv:
        sumFile.write(str(item))
        sumFile.write(" ")
    sumFile.write("\n")
    sumFile.write("Time Cost:\n")
    sumFile.write(str(int(newMPT.endTime-newMPT.startTime)))
    sumFile.write("\n")
    sumFile.write("JSON count:\n")
    sumFile.write(str(len(jsList)) + "\n")
    sumFile.write("Failed count:\n")
    sumFile.write(str(len(newMPT.failList)) + "\n")
    sumFile.write("Faild List:\n")
    if len(newMPT.failList) < 50:
        sumFile.write(pformat(newMPT.failList) + "\n")
    sumFile.close()
    print("All finish!")


if __name__ == "__main__":
    main()
