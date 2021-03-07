#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   Jobs.py
@Desc    :   任务集合
@Version :   1.0
@Time    :   2021/03/06 22:33:25
@Author  :   kevenano 
@Contact :   kevenano@outloook.com
'''

# Here put the import lib
from pathlib import Path
from FUNCTION import download
from CLASS import DB
import os
import time
import threading
import requests
import json


# 全局变量
lock = threading.Lock()
jsDownFailedList = []
finishFlag = 0
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


def downJson(url: str, urlParams: dict, jsonDir: Path):
    '''
    Json下载函数\n
    下载一份Json，并保存到jsonDir下, 以urlParams中的page命名
    '''
    global jsDownFailedList
    global finishFlag
    # 打印提示信息
    if lock.acquire():
        print("Deal with page ", urlParams["page"])
        print()
        lock.release()
    # 尝试下载json
    res = download(url=url, params=urlParams, reFlag=2, timeout=(30, 60))
    if (not isinstance(res, requests.models.Response) or res.status_code != 200) and lock.acquire():
        # 下载失败，更新错误列表
        jsDownFailedList.append(urlParams["page"])
        print(f"Page {urlParams['page']} fail...")
        print()
        lock.release()
    elif len(res.content) < 100 and lock.acquire():
        # 空的json页, 直接判定下载结束
        finishFlag = 1
        print("Page "+str(urlParams["page"])+" empty page!")
        print()
        lock.release()
    else:
        # 下载成功，保存json
        tmpPath = os.path.join(jsonDir, str(urlParams["page"]) + ".json")
        pageFile = open(tmpPath, "w", encoding="utf-8")
        pageFile.write(res.text)
        pageFile.close()
        del tmpPath


def insertData(db: DB, tableName: str, data: dict):
    """
    更新数据库\n
    replace方法\n
    data必须为字典列表(直接由json.load转换得来)\n
    """
    global typeDic
    for work in data:
        for k, v in work.items():
            if typeDic[k] in ["text", "char(10)"]:
                work[k] = str(v)
            if typeDic[k] == "int" and type(v) is not int:
                work[k] = -1
        flag = db.replace(tableName, tuple(work.keys()), tuple(work.values()))
        if flag != 1:
            """更新失败"""
            raise Exception("Faild to insert data!")


def dailyJob(jobDir: Path):
    '''
    日常任务
    1. 连接到数据库
    2. 查询现有最大id号
    3. 多线程下载json数据
    4. 数据库备份
    5. 数据库更新
    6. 数据库回滚
    7. 导出下载地址
    8. 写日志
    9. 发邮件\n
    工作目录结构:\n
    jobDir\n
    └─workDir\n
        └─json\n
    '''
    # 构造工作目录
    workID = int(time.time())
    workDir = os.path.join(jobDir, 'works', str(workID))
    bkDir = os.path.join(jobDir, "sqlBackup")
    jsonDir = os.path.join(workDir, 'json')
    if not os.path.exists(bkDir):
        os.makedirs(bkDir)
    if not os.path.exists(jsonDir):
        os.makedirs(jsonDir)

    # 链接到数据库
    db = DB("localhost", "root", "qo4hr[Pxm7W5", "konachan")
    db.connect()
    db.execute(sql='''SET FOREIGN_KEY_CHECKS = 0 ''')

    # 查询最大id号
    sql = "SELECT MAX(id) FROM main"
    flag = db.execute(sql)
    if flag != 1:
        # print(flag.args[0])
        # Todo: Debug
        db.close()
        # Todo: detail
    maxID = db.fetchall()[0][0]

    # 多线程下载json数据
    startID = maxID + 1
    pageList = [1, 2, 3, 4, 5]  # 先给一个page列表
    threadNum = len(pageList)
    url = "https://konachan.com/post.json"
    params = {"limit": 50, "tags": f"id:>={startID} order:id",
              "page": pageList[0]}
    # 循环获取所有指定页面json
    global finishFlag
    # 目标函数参数设置
    kwargs = {}
    kwargs['jsonDir'] = jsonDir
    while finishFlag == 0:
        pageCnt = 0  # 完成的任务数
        # 创建多线程任务
        thList = []
        for i in range(threadNum):
            params["page"] = pageList[pageCnt]
            kwargs['url'] = url
            kwargs['urlParams'] = params.copy()
            dlThread = threading.Thread(target=downJson, kwargs=kwargs)
            thList.append(dlThread)
            dlThread.start()
            time.sleep(1)
            pageCnt += 1
        # 等待线程结束
        for thread in thList:
            thread.join()
        # 更新pageList
        pageList = [i + len(pageList) for i in pageList]

    # 数据库备份
    bkPath = db.dump(bkDir)

    # 数据库更新
    jsList = []
    insertFail = []
    for folderName, subfolders, fileNames in os.walk(jsonDir):
        for fileName in fileNames:
            if fileName.endswith(".json"):
                jsList.append(os.path.join(folderName, fileName))
    for file in jsList:
        try:
            jsFile = open(file, "r", encoding="utf-8")
            data = json.load(jsFile)
            jsFile.close()
            insertData(db, 'main', data)
        except Exception as e:
            db.execute(sql='''SET FOREIGN_KEY_CHECKS = 1 ''')
            insertFail.append(file)

    # 导出下载地址
    sql = f"SELECT file_url from main WHERE id >= {startID};"
    flag = db.execute(sql)
    if flag != 1:
        results = ()
    else:
        results = db.fetchall()
    # 写入文档
    urlFilePath = os.path.join(workDir, 'url.txt')
    if len(results) > 0:
        with open(urlFilePath, "w", encoding="utf-8") as fn:
            for item in results:
                if item[0] != None:
                    fn.write(str(item[0])+"\n")
    
    # 写日志
    logFilePath = os.path.join(workDir, 'log.txt')

    # 断开数据库连接
    db.execute(sql='''SET FOREIGN_KEY_CHECKS = 1 ''')
    db.close()
    print("All done!")
