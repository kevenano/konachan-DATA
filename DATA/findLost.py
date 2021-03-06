#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   findLost.py
@Desc    :   查找缺失图片
@Version :   1.0
@Time    :   2021/02/19 16:30:19
@Author  :   kevenano 
@Contact :   kevenano@outloook.com
'''

# Hear put the import lib
from pathlib import Path
from CLASS import DB, multiProTask
import os
import sys
from pprint import pformat


class mpTask(multiProTask):
    '多进程任务对象'
    mainFolder = ""
    lostIDlist = []
    lostUrlList = []

    def __init__(self, mainFolder: Path, processNum: int = None) -> None:
        """
        初始化\n
        """
        self.mainFolder = mainFolder
        for subFolder in os.listdir(self.mainFolder):
            self.projectList.append(os.path.join(self.mainFolder, subFolder))
        super().__init__(projectList=self.projectList, processNum=processNum)

    def singlePro(self, task: list) -> None:
        '单进程处理'
        if self.processLock.acquire():
            if len(self.shareDict)==0:
                '初始化共享字典'
                self.shareDict['lostIDlist'] = []
                self.shareDict['lostUrlList'] = []
            self.processLock.release()
        
        db = DB("localhost", "root", "qo4hr[Pxm7W5", "konachan")
        db.connect()
        for imgFolder in task:
            imgList = os.listdir(imgFolder)
            for i in range(len(imgList)):
                imgList[i] = int(imgList[i].split(".")[0])
            imgList=set(imgList)

            year = int(os.path.basename(imgFolder))
            minID, maxID = self.year2range(year)
            sql = f"SELECT id FROM main WHERE id>={minID} AND id<={maxID} AND `status`='active' "
            flag = db.execute(sql)
            if str(flag) != "1":
                self.failList.append(imgFolder)
                if self.processLock.acquire():
                    print("#########################################")
                    print(imgFolder," Fail!")
                    print("#########################################")
                    self.processLock.release()
                continue
            idList = list(db.fetchall())
            for i in range(len(idList)):
                idList[i] = idList[i][0]
            idList = set(idList)

            lostList = list(idList-imgList)
        
            for imgID in lostList:
                # self.AList.append(imgID)
                self.shareDict['lostIDlist'] = self.shareDict['lostIDlist'] + [imgID]
                # 导出下载地址部分
                sql = f"SELECT file_url from main WHERE id={imgID}"
                flag = db.execute(sql)
                if flag != 1:
                    continue
                results = db.fetchall()
                url = results[0][0]
                if url:
                    # self.BList.append(url)
                    self.shareDict['lostUrlList'] = self.shareDict['lostUrlList']+[url]
            
            if self.processLock.acquire():
                print(imgFolder," Done!\n")
                self.processLock.release()

        db.close()

    def year2range(self, year: int) -> tuple:
        '根据year返回ID范围'
        if year == 2009:
            return (1, 50000)
        if year == 2010:
            return (50001, 91915)
        if year == 2011:
            return (91916, 110000)
        if year == 2012:
            return (110001, 151836)
        if year == 2013:
            return (151837, 175606)
        if year == 2014:
            return (175607, 193960)
        if year == 2015:
            return (193961, 210000)
        if year == 2016:
            return (210001, 233400)
        if year == 2017:
            return (233401, 257743)
        if year == 2018:
            return (257744, 276246)
        if year == 2019:
            return (276247, 297318)
        if year == 2020:
            return (297319, 321261)
        if year == 2021:
            return (321262, 999999)
    
    def afterDeal(self) -> None:
        self.lostIDlist = self.shareDict['lostIDlist']
        self.lostUrlList = self.shareDict['lostUrlList']
        self.lostIDlist.sort()
        self.lostUrlList.sort()


def main() -> None:
    print("Running...")
    mainFolder = r"J:\konachan-web\mysite\statics\imageBase"
    newMPT = mpTask(mainFolder,8)
    newMPT.run()

    sumFile = os.path.join(os.getcwd(), str(int(newMPT.endTime)) + "-log.txt")
    urlFile = os.path.join(os.getcwd(),str(int(newMPT.endTime)) + "-urls.txt")
    imgIDfile = os.path.join(os.getcwd(),str(int(newMPT.endTime)) + "-imgID.txt")

    # 写日志
    with open(sumFile, "w", encoding="utf-8") as f:
        for item in sys.argv:
            f.write(str(item))
            f.write(" ")
        f.write("\n")
        f.write("Time Cost:\n")
        f.write(str(int(newMPT.endTime-newMPT.startTime)))
        f.write("\n")

        f.write("Failed count:\n")
        f.write(str(len(newMPT.failList)) + "\n")
        f.write("Faild List:\n")
        if len(newMPT.failList) < 50:
            f.write(pformat(newMPT.failList) + "\n")

        f.write("Lost file count:\n")
        f.write(str(len(newMPT.lostIDlist)) + "\n")
        f.write("Url count:\n")
        f.write(str(len(newMPT.lostUrlList)) + "\n")

    # 写入urls
    with open(urlFile, "w", encoding="utf-8") as f:
        for url in newMPT.lostUrlList:
            f.write(url+"\n")
    with open(imgIDfile, "w", encoding="utf-8") as f:
        for imgID in newMPT.lostIDlist:
            f.write(str(imgID)+"\n")

    print("Done!")


if __name__ == "__main__":
    main()
