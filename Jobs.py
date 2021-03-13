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
import logging
import copy


# 全局变量
lock = threading.Lock()
jsDownFailedList = []
endFlag = 0  # 末页记录
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
    global endFlag
    # 打印提示信息
    if lock.acquire():
        logging.info("Deal with page " + str(urlParams["page"]))
        lock.release()
    # 尝试下载json
    res = download(url=url, params=urlParams, reFlag=2, timeout=(30, 60))
    if (not isinstance(res, requests.models.Response) or res.status_code != 200) and lock.acquire():
        # 下载失败，更新错误列表
        jsDownFailedList.append(urlParams["page"])
        logging.warning(f"Page {urlParams['page']} fail...")
        lock.release()
    elif len(res.content) < 100 and lock.acquire():
        # 空的json页, 直接判定下载结束
        endFlag = 1
        logging.info("Page "+str(urlParams["page"])+" empty page!")
        lock.release()
    else:
        # 下载成功，保存json
        tmpPath = os.path.join(jsonDir, str(urlParams["page"]) + ".json")
        pageFile = open(tmpPath, "w", encoding="utf-8")
        pageFile.write(res.text)
        pageFile.close()
        del tmpPath


def insertData(db: DB, tableName: str, data: dict) -> None:
    """
    更新数据库\n
    replace方法\n
    data必须为字典列表(直接由json.load转换得来)\n
    失败直接报错\n
    """
    global typeDic
    for work in data:
        for k, v in work.items():
            if typeDic[k] in ["text", "char(10)"]:
                work[k] = str(v)
            if typeDic[k] == "int" and type(v) is not int:
                work[k] = -1
        try:
            db.replace(tableName, tuple(work.keys()), tuple(work.values()))
        except Exception as e:
            raise e


def mtDownJson(threadNum: int, startID: int, jsonDir: Path, maxRetry: int) -> bool:
    """
    多线程下载Json\n
    下载完成返回True\n
    否则返回False
    """
    # 参数检查
    if threadNum < 1:
        threadNum = 1
    if startID < 1:
        startID = 1
    if maxRetry < 0:
        maxRetry = 0
    if not jsonDir.is_dir():
        raise "Not a dir!"
    
    logging.info("开始多线程下载json...")
    pageList = [t+1 for t in range(threadNum)]  # 先给一个page列表
    retryCnt = 0  # 失败重试次数
    url = "https://konachan.com/post.json"
    params = {"limit": 50, "tags": f"id:>={startID} order:id",
              "page": pageList[0]}
    
    # 循环获取所有指定页面json
    finishFlag = 0  # 下载结束标志
    global endFlag  # 末页标志
    global jsDownFailedList
    # 目标函数参数设置
    kwargs = {}
    kwargs['jsonDir'] = jsonDir
    while finishFlag == 0:
        if threadNum > len(pageList):
            "修正线程数"
            threadNum = len(pageList)
        # 创建多线程任务
        thList = []
        for i in range(threadNum):
            params["page"] = pageList[i]
            kwargs['url'] = url
            kwargs['urlParams'] = params.copy()
            dlThread = threading.Thread(target=downJson, kwargs=kwargs)
            thList.append(dlThread)
            dlThread.start()
            time.sleep(1)
        # 等待线程结束
        for thread in thList:
            thread.join()
        # 更新pageList
        if endFlag == 0:
            """尚未到达末页"""
            pageList = [len(pageList)+pageList[t] for t in range(threadNum)]
        elif endFlag == 1:
            """已经到达末页"""
            if len(jsDownFailedList) > 0:
                """有失败项"""
                if retryCnt <= maxRetry:
                    """重试次数未超过上限"""
                    logging.info("重新下载失败项...")
                    retryCnt += 1
                    if len(jsDownFailedList) <= threadNum:
                        """失败项数小于进程数"""
                        pageList = copy.copy(jsDownFailedList)
                        jsDownFailedList = []
                    else:
                        """失败项数大于进程数"""
                        for t in range(threadNum):
                            pageList.append(jsDownFailedList.pop(0))
                else:
                    """重试次数超过上限"""
                    logging.warning("重试次数超过上限!")
                    logging.info("剩余失败项："+str(jsDownFailedList.sort()))
                    finishFlag = 1
            else:
                """没有失败项了"""
                logging.info("全部下载完毕！")
                finishFlag = 1
    
    if len(jsDownFailedList) == 0:
        return True
    else:
        return False



def dailyJob(jobDir: Path):
    '''
    日常任务\n
    包含完整的错误处理\n
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
    └─backup\n
        └─json\n
    '''
    # 构造工作目录
    logging.info("正在创建工作目录...")
    workID = int(time.time())
    workDir = os.path.join(jobDir, 'works', str(workID))
    bkDir = os.path.join(jobDir, "backup")
    jsonDir = os.path.join(workDir, 'json')
    if not os.path.exists(bkDir):
        os.makedirs(bkDir)
    if not os.path.exists(jsonDir):
        os.makedirs(jsonDir)
    logging.info("工作目录创建完成！")

    # 链接到数据库
    logging.info("正在连接数据库...")
    db = DB("localhost", "root", "qo4hr[Pxm7W5", "konachan")
    try:
        db.connect()
        logging.info("成功连接到数据库！")
    except Exception as e:
        logging.critical("连接数据库失败！")
        logging.debug(str(e))
        logging.info("********************************************")
    try:
        db.execute(sql='''SET FOREIGN_KEY_CHECKS = 0 ''')
        logging.info("成功设置外键!")
    except Exception as e:
        logging.critical("外键设置错误！")
        logging.debug(str(e))
        logging.info("********************************************")

    # 查询最大id号
    try:
        sql = "SELECT MAX(id) FROM main"
        db.execute(sql)
        maxID = db.fetchall()[0][0]
        logging.info("查询到最大ID："+str(maxID))
    except Exception as e:
        logging.critical("查询最大ID出错！")
        logging.debug(str(e))
        logging.info("********************************************")

    # 多线程下载json数据
    kwargs = {}
    kwargs["threadNum"] = 5
    kwargs["startID"] = maxID+1
    kwargs["jsonDir"] = jsonDir
    kwargs["maxRetry"] = 5
    dlRes = False
    try:
        dlRes = mtDownJson(**kwargs)
    except Exception as e:
        logging.error("下载出错!")
        logging.debug(str(e))
        logging.info("********************************************")
    
    # # 数据库备份
    # bkPath = db.dump(bkDir)

    # # 数据库更新
    # jsList = []
    # insertFail = []
    # for folderName, subfolders, fileNames in os.walk(jsonDir):
    #     for fileName in fileNames:
    #         if fileName.endswith(".json"):
    #             jsList.append(os.path.join(folderName, fileName))
    # for file in jsList:
    #     try:
    #         jsFile = open(file, "r", encoding="utf-8")
    #         data = json.load(jsFile)
    #         jsFile.close()
    #         insertData(db, 'main', data)
    #     except Exception as e:
    #         db.execute(sql='''SET FOREIGN_KEY_CHECKS = 1 ''')
    #         insertFail.append(file)

    # # 导出下载地址
    # sql = f"SELECT file_url from main WHERE id >= {startID};"
    # flag = db.execute(sql)
    # if flag != 1:
    #     results = ()
    # else:
    #     results = db.fetchall()
    # # 写入文档
    # urlFilePath = os.path.join(workDir, 'url.txt')
    # if len(results) > 0:
    #     with open(urlFilePath, "w", encoding="utf-8") as fn:
    #         for item in results:
    #             if item[0] != None:
    #                 fn.write(str(item[0])+"\n")

    # 断开数据库连接
    db.execute(sql='''SET FOREIGN_KEY_CHECKS = 1 ''')
    db.close()
    print("All done!")
