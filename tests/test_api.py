# -*- coding: utf-8 -*-
__author__ = "lvhaidong"

import unittest
import json
import re
from base64 import b64encode
from flask import url_for
from app import create_app, db
from app.models import User, Role, Post, Comment


class APITestCase(unittest.TestCase):

    def setUp(self):
        self.app = create_app('testing')
        self.app_context = self.app.app_context()
        self.app_context.push()
        db.create_all()
        Role.insert_roles()
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.app_context.pop()

    def get_api_headers(self, username, password):
        return {
            'Authorization': 'Basic ' + b64encode((username + ':' + password).encode('utf-8')).decode('utf-8'),
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def test_no_auth(self):
        response = self.client.get(url_for('api.getPosts'), content_type='application/json')
        self.assertTrue(response.status_code == 401)

    def test_posts(self):
        # 添加一个用户
        r = Role.query.filter_by(name='User').first()
        self.assertIsNotNone(r)
        u = User(email='lvhaidong520@126.com', password='123456', confirmed=True, role=r)
        db.session.add(u)
        db.session.commit()

        # 写一个博客
        response = self.client.post(url_for('api.new_post'),  headers=self.get_auth_header('lvhaidong520@126.com', '123456'),
                 data=json.dumps({'body': 'body of the *blog* post'}))
        self.assertTrue(response.status_code == 201)
        url = response.headers.get('Location')
        self.assertIsNotNone(url)

        # 获取刚发布的文章
        response = self.client.get(url, headers=self.get_auth_header('lvhaidong520@126.com', '123456'))
        self.assertTrue(response.status_code == 200)
        json_response = json.loads(response.data.decode('utf-8'))
        self.assertTrue(json_response['url'] == url)
        self.assertTrue(json_response['body'] == 'body of the *blog* post')
        self.assertTrue(json_response['body_html'] == '<p>body of the <em>blog</em> post</p>')
