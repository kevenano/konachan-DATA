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
from time import time, sleep
from apscheduler.schedulers.background import BackgroundScheduler
from Jobs import dailyJob, dataUpdate
import sys
import os
import cryptography
import configparser
from pprint import pprint
import copy


# 计划部分代码
def schedulerPart(jobDir: str, dbPar: dict, mailPar: dict, trigerPar: dict, proxies: dict, jobFlag: int = 1):
    '''
    计划部分代码
    每天6点执行一次job中的业务
    '''
    scheduler = BackgroundScheduler()
    # 读入配置文件
    kwargs = {}
    kwargs['jobDir'] = jobDir
    kwargs['dbPar'] = dbPar
    kwargs['mailPar'] = mailPar
    kwargs['proxies'] = proxies
    if jobFlag == 1:
        scheduler.add_job(func=dailyJob, trigger='cron',
                          **trigerPar, kwargs=copy.copy(kwargs))
        kwargs['startID'] = 250000
        kwargs['endID'] = 999999
        scheduler.add_job(func=dataUpdate, trigger='cron',day_of_week='mon',hour=18,kwargs=copy.copy(kwargs))
    elif jobFlag == 2:
        kwargs['startID'] = 1
        kwargs['endID'] = 999999
        scheduler.add_job(func=dataUpdate, kwargs=copy.copy(kwargs))
    elif jobFlag == 3:
        kwargs['startID'] = 250000
        kwargs['endID'] = 999999
        scheduler.add_job(func=dataUpdate, kwargs=copy.copy(kwargs))
    elif jobFlag == 4:
        scheduler.add_job(func=dailyJob, kwargs=copy.copy(kwargs))
    scheduler.start()


def readConfig(confFile: str = 'server.conf'):
    '''
    读取配置文件
    '''
    cf = configparser.ConfigParser()
    cf.read(confFile)

    jobDir = ''
    dbPar = {}
    mailPar = {}
    trigerPar = {}
    proxies = {}

    if os.name == 'nt':
        jobDir = cf.get('JOBDIR', 'windows')
    else:
        jobDir = cf.get('JOBDIR', 'linux')

    dbPar['host'] = cf.get('MYSQL', 'host')
    dbPar['port'] = cf.get('MYSQL', 'port')
    dbPar['user'] = cf.get('MYSQL', 'user')
    dbPar['passwd'] = cf.get('MYSQL', 'passwd')
    dbPar['database'] = cf.get('MYSQL', 'database')

    mailPar['sender'] = cf.get('MAIL', 'sender')
    mailPar['passwd'] = cf.get('MAIL', 'passwd')
    mailPar['receivers'] = cf.get('MAIL', 'receivers').split(',')

    trigerPar['year'] = cf.get('TRIGER', 'year')
    trigerPar['month'] = cf.get('TRIGER', 'month')
    trigerPar['day'] = cf.get('TRIGER', 'day')
    trigerPar['week'] = cf.get('TRIGER', 'week')
    trigerPar['day_of_week'] = cf.get('TRIGER', 'day_of_week')
    trigerPar['hour'] = cf.get('TRIGER', 'hour')
    trigerPar['minute'] = cf.get('TRIGER', 'minute')

    proxies["http"] = cf.get('PROXY', 'http')
    proxies["https"] = cf.get('PROXY', 'https')

    return jobDir, dbPar, mailPar, trigerPar, proxies


# 入口
if __name__ == "__main__":
    # 任务选择
    if len(sys.argv) < 2:
        print("Usage: python serverStaff.py [jobName]")
        exit()
    jobName = sys.argv[1]
    # print(jobName)
    jobFlag = 0
    if jobName.lower() == "daily":
        jobFlag = 1
    elif jobName.lower() == "full":
        jobFlag = 2
    elif jobName.lower() == "part":
        jobFlag = 3
    elif jobName.lower() == "now":
        jobFlag = 4
    else:
        print("No such job!")
        exit()

    # 读取配置
    jobDir, dbPar, mailPar, trigerPar, proxies = readConfig()

    # 预处理
    if not os.path.isdir(jobDir):
        os.makedirs(jobDir)

    # 启动服务器进程
    schedulerPart(jobDir, dbPar, mailPar, trigerPar, proxies, jobFlag)
    sleep(31536000)
    # sleep(3600)
