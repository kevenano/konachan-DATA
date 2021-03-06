#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   serverStaff.py
@Desc    :   服务器端自动化业务
@Version :   1.0
@Time    :   2021/03/06 16:51:31
@Author  :   kevenano 
@Contact :   kevenano@outloook.com
'''

# Here put the import lib
from FUNCTION import download
from CLASS import DB
from apscheduler.schedulers.background import BackgroundScheduler
import time


# 业务部分代码
def job():
    print(int(time.time()))


# 计划部分代码
def schedulerPart():
    '''
    计划部分代码
    '''
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=job,trigger='interval',seconds=3)
    scheduler.start()
    


# 入口
if __name__=="__main__":
    schedulerPart()
    time.sleep(100)
