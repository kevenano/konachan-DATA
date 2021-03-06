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


lock = threading.Lock()


# 全局变量
jsDownFailedList = []
finishFlag = 0


def downJson(url: str, urlParams: dict, jsonPath: Path):
    '''
    Json下载函数\n
    下载一份Json，并保存到jsonPath下, 以urlParams中的page命名
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
        tmpPath = os.path.join(jsonPath, str(urlParams["page"]) + ".json")
        pageFile = open(tmpPath, "w", encoding="utf-8")
        pageFile.write(res.text)
        pageFile.close()
        del tmpPath


def dailyJob(jsonPath: Path):
    '''
    日常任务
    1. 连接到数据库
    2. 查询现有最大id号
    3. 多线程下载json数据
    4. 数据库更新
    5. 导出下载地址
    6. 写日志
    7. 发邮件
    '''
    # 链接到数据库
    db = DB("localhost", "root", "qo4hr[Pxm7W5", "konachan")
    db.connect()

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
    kwargs['jsonPath'] = jsonPath
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


    # 断开数据库连接
    db.close()
    print("All done!")
