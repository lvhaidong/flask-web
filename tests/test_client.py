# -*- coding: utf-8 -*-
__author__ = "lvhaidong"

import re
import unittest
from flask import url_for
from app import create_app, db
from app.models import User, Role


class FlaskClientTestCase(unittest.TestCase):
    def setUp(self):
        self.app = create_app('testing')
        self.app_context = self.app.app_context()
        self.app_context.push()
        db.create_all()
        Role.insert_roles()
        # as_text=True 得到的是Unicode 字符串
        self.client = self.app.test_client(use_cookies=True)

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.app_context.pop()

    def test_home_page(self):
        response = self.client.get(url_for('main.index'))
        self.assertTrue('Stranger' in response.get_data(as_text=True))

    def test_register_and_login(self):
        # register a new account
        response = self.client.post(url_for('auth.register'), data={
            'email': 'lvhaidong520@126.com',
            'username': 'lhd1',
            'password': '123456',
            'password2': '123456'
        })
        self.assertTrue(response.status_code == 302)

        # 使用新注册的账户登录
        response = self.client.post(url_for('auth.login'), data={
            'email': 'lvhaidong520@126.com',
            'password': '123456'}, follow_redirects=True)
        data = response.get_data(as_text=True)
        self.assertTrue(re.search('Hello,\s+john!', data))
        self.assertTrue('You have not confirmed your account yet' in data)

        # 发送确认令牌
        user = User.query.filter_by(email='lvhaidong520@126.com').first()
        token = user.generate_confirmation_token()
        # follow_ redirects=True，让测试客户端和浏览器一样，自动向重定向的URL发起GET请求。指定这个参数后,返回的不是302状态码，而是请求重定向的URL返回的响应。
        response = self.client.get(url_for('auth.confirm', token=token), follow_redirects=True)
        data = response.get_data(as_text=True)
        self.assertTrue('You have confirmed your account' in data)

        # 退出
        response = self.client.get(url_for('auth.logout'), follow_redirects=True)
        data = response.get_data(as_text=True)
        self.assertTrue('You have been logged out' in data)