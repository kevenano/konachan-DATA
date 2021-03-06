# 文件名处理同时生成缩略图
import os
import time
import sys
import threading
from PIL import Image
from elasticsearch import Elasticsearch
from image_match.elasticsearch_driver import SignatureES
from tqdm import tqdm
from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

lock = threading.Lock()

# 初始化错误列表
failedList = []

# 记录时间 创建工作目录
startTime = time.time()
taskId = int(startTime)
taskPath = os.path.join(os.getcwd(), str(taskId))
os.mkdir(taskPath)


# 单文件重命名
# 成功返回1，否则返回-1
def reName(filePath):
    fileName = os.path.basename(filePath)
    items = fileName.split(" ")
    sufix = fileName.split(".")[-1]
    outPath = filePath
    try:
        fileID = int(items[2])
    except Exception as e:
        print(e)
        return -1, outPath
    try:
        outPath = filePath.replace(fileName, str(fileID)+"."+sufix)
        os.rename(filePath, outPath)
        return 1, outPath
    except Exception as e:
        print(e)
        return -1, outPath


# 单文件生成缩略图
# 成功返回1，否则返回-1
def mkThumb(filePath, outFolder, outSize=(200, 200)):
    try:
        fileName = os.path.basename(filePath)
        outName = "thumb-" + fileName.split(".")[0] + ".jpg"
        outPath = os.path.join(outFolder, outName)
        im = Image.open(filePath)
        im.thumbnail(outSize)
        im = im.convert("RGB")
        im.save(outPath, "JPEG")
        return 1, outPath
    except Exception as e:
        print(e)
        return -1, ""


# 结合重命名和生成缩略图
# 多线程处理
def reAndThum(filePath, outFolder, outSize=(200, 200)):
    global failedList
    # 尝试处理该文件
    flag1, outPath = reName(filePath)
    if flag1 == -1 and lock.acquire():
        # 更新错误列表
        failedList.append(filePath)
        print(os.path.basename(outPath), "failed.")
        print()
        lock.release()
    else:
        flag2, thumbPath = mkThumb(outPath, outFolder, outSize)
        if flag2 == -1 and lock.acquire():
            # 更新错误列表
            failedList.append(filePath)
            print(os.path.basename(outPath), "failed.")
            print()
            lock.release()
        else:
            # 向图像匹配库中添加该图片(使用缩略图)
            try:
                es = Elasticsearch()
                ses = SignatureES(es)
                ses.add_image(thumbPath)
            except Exception:
                if lock.acquire():
                    failedList.append(filePath)
                    print(os.path.basename(outPath), "Failed to add to image-match database.")
                    print()
                    lock.release()


# 向elasticSearch中添加图片
def storeImage(inDir):
    es = Elasticsearch()
    ses = SignatureES(es)
    # 获取文件列表
    fileList = []
    for folderName, subfolders, fileNames in os.walk(inDir):
        for fileName in fileNames:
            if fileName.endswith(("jpg", "png", "jpeg", "gif")):
                fileList.append(os.path.join(folderName, fileName))
    # 循环处理
    pbar = tqdm(fileList, ncols=100)
    cnt = 0
    for imagePath in pbar:
        cnt += 1
        imageID = int(os.path.basename(imagePath).split(".")[0])
        pbar.set_description(f"Deal with {imageID}")
        # image = cv2.imread(imagePath)
        metadata = {"imageID": imageID}
        ses.add_image(path=imagePath, metadata=metadata)


# 主函数
def main():
    # 输入参数处理
    if len(sys.argv) < 4:
        print('Usage: python insertData.py [inDir] [outDir] [threadNum]')
        exit()
    inDir = sys.argv[1]
    outDir = sys.argv[2]
    threadNum = int(sys.argv[3])
    if threadNum < 1:
        threadNum = 1
    elif threadNum > 128:
        threadNum = 128
    # 开始处理
    fileList = []
    fileCnt = 0
    print("正在获取文件列表...")
    for folderName, subfolders, fileNames in os.walk(inDir):
        for fileName in fileNames:
            if fileName.endswith(("jpg", "png", "jpeg", "gif")) and fileName.startswith("Konachan.com - "):
                fileList.append(os.path.join(folderName, fileName))
    # 多线程循环处理所有文件
    print("正在处理...")
    while fileCnt < len(fileList):
        # 创建多线程任务
        if threadNum > len(fileList) - fileCnt:
            threadNum = len(fileList) - fileCnt
        thList = []
        for i in range(threadNum):
            wkThread = threading.Thread(
                target=reAndThum, args=(fileList[fileCnt], outDir)
            )
            thList.append(wkThread)
            wkThread.start()
            fileCnt += 1
        # 等待线程结束
        for thread in thList:
            thread.join()
    # 写日志
    writeLog(fileCnt)
    # 完成
    print("All finish!")


# 写日志
def writeLog(fileCnt):
    global startTime
    global taskId
    global taskPath
    global failedList
    logPath = os.path.join(taskPath, "log.txt")

    endTime = time.time()
    spendTime = int(endTime - startTime)
    logFile = open(logPath, "w")
    logFile.write("Task ID:\n")
    logFile.write(str(taskId) + "\n")
    logFile.write("File Count:\n")
    logFile.write(str(fileCnt) + "\n")
    logFile.write("Failed Count:\n")
    logFile.write(str(len(failedList)) + "\n")
    logFile.write("Failed List:\n")
    logFile.write(str(failedList).replace(",", "\n") + "\n")
    logFile.write("Start Time:\n")
    logFile.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(startTime)) + "\n")
    logFile.write("End Time:\n")
    logFile.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTime)) + "\n")
    logFile.write("Time Cost:\n")
    logFile.write(str(spendTime // 3600) + "h")
    logFile.write(str((spendTime % 3600) // 60) + "m")
    logFile.write(str(spendTime % 60) + "s" + "\n")
    logFile.close()


# test
def test():
    inDir = "I:\\image\\2009"
    for folderName, subfolders, fileNames in os.walk(inDir):
        for fileName in fileNames:
            if fileName.endswith("jpeg"):
                inPath = os.path.join(folderName, fileName)
                outPath = inPath.replace("jpeg", ".jpeg")
                os.rename(inPath, outPath)


if __name__ == "__main__":
    main()
    # test()
