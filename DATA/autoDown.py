# konachan自动更新程序
# 导出链接（鉴于程序下载速度太慢，顾采用外置程序下载）
# 本程序只负责查询服务器端的新数据，并更新本地数据库（插入新数据）
import requests
import os
import time
import threading
import my_fake_useragent as mfua
import copy
import json
from pprint import pformat
from tqdm import tqdm
from CLASS_DB import DB

lock = threading.Lock()

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

# 记录时间 创建工作目录
startTime = time.time()
taskId = int(startTime)
taskPath = os.path.join(os.getcwd(), str(taskId))
os.mkdir(taskPath)
jsonPath = os.path.join(taskPath, "json")
os.mkdir(jsonPath)
urlFilePath = os.path.join(taskPath, "urls.txt")
logPath = os.path.join(taskPath, "log.txt")

# 初始化下载错误列表和下载结束标志
jsDownFailedList = []
finishFlag = 0


# Html download function
# 输入参数reFlag = 0 返回text, 1 返回content, 2 返回resp
# 如下载错误，将返回resp=None
def download(
    url, num_retries=3, headers={}, cookie="", params="", reFlag=0, timeout=(30, 300),
):
    # print("Downloading: ", url)
    if "user-agent" not in headers:
        headers["user-agent"] = mfua.UserAgent().random()
    if cookie != "":
        headers["cookie"] = cookie
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        resp.close()
        html = resp.text
        content = resp.content
        if resp.status_code >= 400:
            print("Download error: ", resp.text)
            html = None
            content = None
            if num_retries and 500 <= resp.status_code < 600:
                return download(url, num_retries - 1)
    except requests.exceptions.RequestException as e:
        print("Download error!!!")
        print(e)
        html = None
        content = None
        resp = None
    except requests.exceptions.Timeout:
        print("请求超时!")
        html = None
        content = None
    if reFlag == 0:
        return html
    elif reFlag == 1:
        return content
    else:
        return resp


# 多线程下载保存json
def downJson(url, urlParams):
    global jsDownFailedList
    global jsonPath
    global finishFlag
    # 打印提示信息
    if lock.acquire():
        print("Deal with page ", urlParams["page"])
        print()
        lock.release()
    # 尝试下载json
    res = download(url=url, params=urlParams, reFlag=2, timeout=(30, 60))
    if (res is None or res.status_code != 200) and lock.acquire():
        # 更新错误列表
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
        # 保存json
        tmpPath = os.path.join(jsonPath, str(urlParams["page"]) + ".json")
        pageFile = open(tmpPath, "w", encoding="utf-8")
        pageFile.write(res.text)
        pageFile.close()
        del tmpPath


# 更新数据库
# data必须为字典列表(直接由json.load转换得来)
# 返回操作失败的项目id
def updateData(db, tableName, data):
    global typeDic
    failedList = []
    for work in data:
        for k, v in work.items():
            if typeDic[k] in ["text", "char(10)"]:
                work[k] = str(v)
            if typeDic[k] == "int" and type(v) is not int:
                work[k] = -1
            if k == "id":
                continue
            flag = db.update(tableName, k, work[k], r"where `id`=" + str(work["id"]))
            if flag != 1:
                failedList.append(work["id"])
                break
    return failedList


# 向数据库中插入data
# data必须为字典列表(直接由json.load转换得来)
def insertData(db, tableName, data):
    global typeDic
    dupList = []
    failedList = []
    for work in data:
        for k, v in work.items():
            if typeDic[k] in ["text", "char(10)"]:
                work[k] = str(v)
            if typeDic[k] == "int" and type(v) is not int:
                work[k] = -1
        flag = db.insert(tableName, tuple(work.keys()), tuple(work.values()))
        if flag != 1:
            if flag.args[0] == 1062:
                # 主键冲突 尝试更新
                dupList.append(work["id"])
                fl = updateData(db, tableName, [work])
                if len(fl) > 0:
                    # 更新失败
                    for item in fl:
                        failedList.append(work["id"])
            else:
                failedList.append(work["id"])
    return failedList, dupList


# 主函数
def main():
    global startTime
    global taskId
    global taskPath
    global jsonPath
    global urlFilePath
    global logPath
    global jsDownFailedList
    global finishFlag

    # 链接到数据库
    db = DB("localhost", "root", "qo4hr[Pxm7W5", "konachan")
    db.connect()
    # 查询最大id号
    sql = "SELECT MAX(id) FROM main"
    flag = db.execute(sql)
    if flag != 1:
        print(flag.args[0])
        exit()
    result = db.fetchall()[0][0]

    # json下载部分
    print("开始下载json:")
    startId = result + 1
    pageList = [1, 2, 3, 4, 5]  # 先给一个page列表
    threadNum = len(pageList)
    url = "https://konachan.com/post.json"
    params = {"limit": 50, "tags": f"id:>={startId} order:id", "page": pageList[0]}
    # 循环获取所有指定页面json
    while finishFlag == 0:
        pageCnt = 0  # 完成的任务数
        # 创建多线程任务
        thList = []
        for i in range(threadNum):
            params["page"] = pageList[pageCnt]
            dlThread = threading.Thread(target=downJson, args=(url, params.copy()))
            thList.append(dlThread)
            dlThread.start()
            time.sleep(1)
            pageCnt += 1
        # 等待线程结束
        for thread in thList:
            thread.join()
        # 更新pageList
        pageList = [i + len(pageList) for i in pageList]

    # 更新数据库部分
    print("正在写入数据库...")
    jsList = []
    insertFailedDic = {}
    dupDic = {}
    cnt = 0
    for folderName, subfolders, fileNames in os.walk(jsonPath):
        for fileName in fileNames:
            if fileName.endswith(".json"):
                jsList.append(os.path.join(folderName, fileName))
    pbar = tqdm(jsList, ncols=100)
    for file in pbar:
        cnt += 1
        pbar.set_description(f"{cnt} of {len(jsList)}")
        jsFile = open(file, "r", encoding="utf-8")
        data = json.load(jsFile)
        jsFile.close()
        insertFailedList, dupList = insertData(db, "main", data)
        if len(insertFailedList) > 0:
            insertFailedDic[file] = copy.copy(insertFailedList)
        if len(dupList) > 0:
            dupDic[file] = copy.copy(dupList)

    # 导出下载地址部分
    sql = f"SELECT file_url from main WHERE id >= {startId};"
    flag = db.execute(sql)
    if flag != 1:
        results = (("Null",),)
    else:
        results = db.fetchall()
    # 写入文档
    if len(results) > 0:
        with open(urlFilePath, "w", encoding="utf-8") as fn:
            for item in results:
                fn.write(str(item[0])+"\n")

    # 写日志
    # 查询最大id号
    sql = "SELECT MAX(id) FROM main"
    flag = db.execute(sql)
    if flag != 1:
        print(flag.args[0])
        endId = "Unknown"
    else:
        endId = db.fetchall()[0][0]
    endTime = time.time()
    spendTime = int(endTime - startTime)
    logFile = open(logPath, "w")
    logFile.write("Task ID:\n")
    logFile.write(str(taskId) + "\n")
    logFile.write("Deafault Params:\n")
    logFile.write("limit: 50 threads: 5" + "\n")
    logFile.write("ID Range:\n")
    logFile.write(str(startId) + "-" + str(endId) + "\n")
    logFile.write("Failed Page:\n")
    logFile.write(str(len(jsDownFailedList)) + "\n")
    logFile.write("List:\n")
    logFile.write(str(jsDownFailedList) + "\n")
    logFile.write("Insert Failed Count:\n")
    logFile.write(str(len(insertFailedDic)) + "\n")
    logFile.write("List:\n")
    logFile.write(pformat(insertFailedDic) + "\n")
    logFile.write("Duplicated Count:\n")
    logFile.write(str(len(dupDic)) + "\n")
    logFile.write("List:\n")
    logFile.write(pformat(dupDic) + "\n")
    logFile.write("Start Time:\n")
    logFile.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(startTime)) + "\n")
    logFile.write("End Time:\n")
    logFile.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTime)) + "\n")
    logFile.write("Time Cost:\n")
    logFile.write(str(spendTime // 3600) + "h")
    logFile.write(str((spendTime % 3600) // 60) + "m")
    logFile.write(str(spendTime % 60) + "s" + "\n")
    logFile.close()

    # 断开数据库链接
    db.close()

    # 结束
    print("All finish!")


if __name__ == "__main__":
    main()
