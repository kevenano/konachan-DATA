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
from time import time
from apscheduler.schedulers.background import BackgroundScheduler
from Jobs import dailyJob

# 计划部分代码
def schedulerPart():
    '''
    计划部分代码
    每天6点执行一次job中的业务
    '''
    scheduler = BackgroundScheduler()
    # scheduler.add_job(func=dailyJob,trigger='cron', hour=6)
    # 目标函数参数设置
    kwargs = {}
    # kwargs['jobDir'] = r"E:\konachan\dev\Test"
    kwargs['jobDir'] = r"/home/kevin/Work/Test"
    kwargs['host'] = 'localhost'
    kwargs['user'] = 'root'
    kwargs['passwd'] = 'qhm2012@@@'
    kwargs['database'] = 'konachan'
    scheduler.add_job(func=dailyJob, kwargs=kwargs)
    scheduler.start()
    

# 入口
if __name__=="__main__":
    schedulerPart()
    while True:
        pass
