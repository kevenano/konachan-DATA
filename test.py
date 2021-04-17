#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   test.py
@Desc    :   测试用
@Version :   2.0
@Time    :   2021/04/17 16:33:30
@Author  :   kevenano 
@Contact :   kevenano@outloook.com
'''

# Here put the import lib
import configparser
import os


cf = configparser.ConfigParser()
cf.read('server.conf')

secs = cf.sections()

print(secs)

opts = cf.options('MYSQL')

print(opts)

host = cf.get('MYSQL','host')

print(host)