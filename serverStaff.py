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
from CLASS import test
from time import time
from apscheduler.schedulers.background import BackgroundScheduler
from Jobs import dailyJob
import sys
import os
import cryptography

# 计划部分代码
def schedulerPart(jobDir:str, testFlag:int=1):
    '''
    计划部分代码
    每天6点执行一次job中的业务
    '''
    scheduler = BackgroundScheduler()
    # 目标函数参数设置
    kwargs = {}
    kwargs['jobDir'] = jobDir
    kwargs['host'] = 'localhost'
    kwargs['user'] = 'kevin'
    kwargs['passwd'] = '1972774684'
    kwargs['database'] = 'konachan'
    if testFlag:
        scheduler.add_job(func=dailyJob, kwargs=kwargs)
    else:
        scheduler.add_job(func=dailyJob, trigger='cron', hour=8, kwargs=kwargs)
    scheduler.start()
    

# 入口
if __name__=="__main__":
    # 预处理
    if len(sys.argv)<3:
        print("Usage: python serverStaff.py [jobDir] [testFlag]")
        exit()
    jobDir = sys.argv[1]
    if not os.path.isdir(jobDir):
        os.makedirs(jobDir)
    os.chdir(jobDir)
    testFlag = int(sys.argv[2])

    # 启动服务器进程
    schedulerPart(jobDir,testFlag)
    while True:
        pass
