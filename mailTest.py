#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   mailTest.py
@Desc    :   邮件发送测试
@Version :   1.0
@Time    :   2021/03/15 03:43:13
@Author  :   kevenano
@Contact :   kevenano@outloook.com
'''

# Here put the import lib
# coding:utf-8
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header


class QQMail:
    """
    QQ邮箱SMTP客户端\n
    """
    # 创建一个带附件的实例
    message = MIMEMultipart()

    def __init__(self, sender: str, passwd: str, receivers: list):
        """
        初始化\n
        """
        self.mail_host = "smtp.qq.com"
        self.sender = sender
        self.mail_pass = passwd
        self.receivers = receivers

    def creatMessage(self):
        """创建邮件"""
        # self.message['From'] = Header("菜鸟教程", 'utf-8')
        # self.message['To'] =  Header("测试", 'utf-8')
        # subject = 'Python SMTP 邮件测试'
        # self.message['Subject'] = Header(subject, 'utf-8')

        # #邮件正文内容
        # self.message.attach(MIMEText('这是菜鸟教程Python 邮件发送测试……', 'plain', 'utf-8'))

        # # 构造附件1，传送当前目录下的 test.txt 文件
        # att1 = MIMEText(open('test.txt', 'rb').read(), 'base64', 'utf-8')
        # att1["Content-Type"] = 'application/octet-stream'
        # # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        # att1["Content-Disposition"] = 'attachment; filename="test.txt"'
        # self.message.attach(att1)

        # # 构造附件2，传送当前目录下的 runoob.txt 文件
        # att2 = MIMEText(open('runoob.txt', 'rb').read(), 'base64', 'utf-8')
        # att2["Content-Type"] = 'application/octet-stream'
        # att2["Content-Disposition"] = 'attachment; filename="runoob.txt"'
        # self.message.attach(att2)
    
    def addAtt(self,filePath:str,name:str):
        """添加文件到附件"""
        with open(filePath,'rb') as f:
            att = MIMEApplication(f.read())
        att.add_header('Content-Disposition', 'attachment', filename=name)
        self.message.attach(att)
    
    def send(self):
        """发送邮件"""
        self.creatMessage()
        smtpObj = smtplib.SMTP_SSL(self.mail_host, 465)
        smtpObj.login(self.sender, self.mail_pass)
        smtpObj.sendmail(self.sender, self.receivers, self.message.as_string())
        smtpObj.quit()


class Mail(QQMail):
    """测试"""
    def creatMessage(self):
        """创建邮件"""
        # 邮件头
        self.message['From'] = Header('kevin-u', 'utf-8')
        self.message['To'] = Header('kevin-t', 'utf-8')
        subject = 'Python SMTP 邮件测试'
        self.message['Subject'] = Header(subject, 'utf-8')

        # 邮件正文
        content = MIMEText('This is a test mail from kevin at ubuntu vm...','plain','utf-8')
        self.message.attach(content)

        # 添加附件
        self.addAtt(r"/home/kevin/Work/konachan-DATA/brief.log",'brief')
        self.addAtt(r"/home/kevin/Work/konachan-DATA/detail.log",'detail')


if __name__ == '__main__':
    mail = Mail(sender='kevin-san@qq.com',passwd='gyuyuzsewsqaebba',receivers=["kevin_ali@aliyun.com"])
    mail.send()
