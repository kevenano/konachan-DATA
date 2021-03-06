#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   FUNCTION.py
@Desc    :   常用函数库
@Version :   1.0
@Time    :   2021/03/06 16:40:58
@Author  :   kevenano 
@Contact :   kevenano@outloook.com
'''

# Here put the import lib
import requests
import my_fake_useragent as mfua


def download(
    url, num_retries=3, headers={}, cookie="", params="", reFlag=0, timeout=(30, 300),
):
    '''
    下载函数\n
    输入参数reFlag = 0 返回text, 1 返回content, 2 返回resp\n
    如下载错误，将返回resp=None\n
    '''
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
            # print("Download error: ", resp.text)
            html = None
            content = None
            if num_retries and 500 <= resp.status_code < 600:
                return download(url, num_retries - 1)
    except requests.exceptions.RequestException as e:
        # print("Download error!!!")
        # print(e)
        html = None
        content = None
        resp = None
    except requests.exceptions.Timeout:
        # print("请求超时!")
        html = None
        content = None
    if reFlag == 0:
        return html
    elif reFlag == 1:
        return content
    else:
        return resp
