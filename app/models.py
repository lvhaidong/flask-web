# -*- coding: utf-8 -*-
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from . import db
from flask_login import UserMixin, AnonymousUserMixin
from . import login_manager
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from flask import current_app, request
import hashlib
from markdown import markdown
import bleach


"""
     操  作                     位  值                      说  明
  关 注 用 户                0b00000001(0x01)          关注其他用户
  在他人的文章中发表评论       0b00000010(0x02)          在他人撰写的文章中发布评论
  写文章                    0b00000100(0x04)           写原创文章
  管理他人发表的评论          0b00001000(0x08)           查处他人发表的不当评论
  管理员权限                 0b10000000(0x80)           管理网站
"""


class Permission(object):
    FOLLOW = 0x01  # 关注其他用户
    COMMENT = 0x02  # 在他人撰写的文章中发布评论
    WRITE_ARTICLES = 0x04  # 写原创文章
    MODERATE_COMMENTS = 0x08  # 查处他人发表的不当评论
    ADMINISTER = 0x80  # 管理网站


class Follow(db.Model):
    """关注者类"""
    __tablename__ = 'follows'
    follower_id = db.Column(db.Integer, db.ForeignKey('users.id'), primary_key=True)
    followed_id = db.Column(db.Integer, db.ForeignKey('users.id'), primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.now)


class User(UserMixin, db.Model):
    """用户"""
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(64), unique=True, index=True)
    username = db.Column(db.String(64), unique=True, index=True)
    password_hash = db.Column(db.String(128))
    confirmed = db.Column(db.Boolean, default=False)
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
    # 用户的真实姓名、所在地、自我介绍、注册日期和最后访问日期
    name = db.Column(db.String(64))
    location = db.Column(db.String(64))
    about_me = db.Column(db.Text())
    member_since = db.Column(db.DateTime(), default=datetime.now)
    last_seen = db.Column(db.DateTime(), default=datetime.now)
    # img 散列值
    avatar_hash = db.Column(db.String(32))
    posts = db.relationship('Post', backref='author', lazy="dynamic")

    """
        1.为了消除外键间的歧义，定义关系时必须使用可选参数foreign_keys指定的外键
        2.db.backref()参数并不是指定这两个关系之间的引用关系,而是回引Follow模型。
          回引中的lazy参数指定为joined,lazy 模式可以实现立即从联结查询中加载相关对象
          设定为 lazy='joined' 模式，就可在一次数据库查询中完成这些操作
        3.lazy 参数都在“一”这一侧设定， 返回的结果是“多”这一侧中的记录。
          上述代码使用的是dynamic，因此关系属性不会直接返回记录，而是返回查询对象，所以在执行查询之前还可以添加额外的过滤器。
        4. cascade层叠选项值 delete-orphan的作用是:启用所有默认层叠选项，而且还要删除孤儿记录。
    """
    followed = db.relationship('Follow', foreign_keys=[Follow.follower_id],
                               backref=db.backref('follower', lazy='joined'),
                               lazy='dynamic', cascade='all, delete-orphan')
    followers = db.relationship('Follow', foreign_keys=[Follow.followed_id],
                                backref=db.backref('followed', lazy='joined'),
                                lazy='dynamic', cascade='all, delete-orphan')

    def __repr__(self):
        return '<User %r>' % self.username

    def __init__(self, **kwargs):
        super(User, self).__init__(**kwargs)
        if self.role is None:
            if self.email == current_app.config['FLASKY_ADMIN']:
                self.role = Role.query.filter_by(permissions=0xff).first()
            else:
                self.role = Role.query.filter_by(default=True).first()

        if self.email is not None and self.avatar_hash is None:
            self.avatar_hash = hashlib.md5(self.email.encode('utf-8')).hexdigest()

    @property
    def password(self):
        raise AttributeError('password is not a readable attribute')

    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def generate_confirmation_token(self, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'confirm': self.id})

    def confirm(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except Exception as e:
            return False
        # 令牌中的 id 是否和存储在 current_user中的已登录用户匹配
        if data.get('confirm') != self.id:
            return False
        self.confirmed = True
        db.session.add(self)
        return True

    # 检查权限的问题
    def can(self, permissions):
        return self.role is not None and (self.role.permissions & permissions) == permissions

    # 检查是否是管理员
    def is_administrator(self):
        return self.can(Permission.ADMINISTER)

    # 更新已登录用户的访问时间
    def ping(self):
        self.last_seen = datetime.now()
        db.session.add(self)
        db.session.commit()

    def change_email(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return False

        if data.get('change_email') != self.id:
            return False
        new_email = data.get('new_email')
        if new_email is None:
            return False
        if self.query.filter_by(email=new_email).first() is not None:
            return False
        self.email = new_email
        self.avatar_hash = hashlib.md5(self.email.encode('utf-8')).hexdigest()
        db.session.add(self)
        return True

    def gravatar(self, size=100, default='identicon', rating='g'):
        if request.is_secure:
            url = 'https://secure.gravatar.com/avatar'
        else:
            url = 'http://www.gravatar.com/avatar'
        hash = self.avatar_hash or hashlib.md5(self.email.encode('utf-8')).hexdigest()
        return '{url}/{hash}?s={size}&d={default}&r={rating}'.format(
            url=url, hash=hash, size=size, default=default, rating=rating)

    # 生成虚拟用户
    @staticmethod
    def generate_fake(count=100):
        from sqlalchemy.exc import IntegrityError
        from random import seed
        import forgery_py

        # 改变随机数生成器的值,可以指定值
        seed()
        for i in range(count):
            u = User(email=forgery_py.internet.email_address(), username=forgery_py.internet.user_name(True),
                     password=forgery_py.lorem_ipsum.word(), confirmed=True, name=forgery_py.name.full_name(),
                     location=forgery_py.address.city(), about_me=forgery_py.lorem_ipsum.sentence(),
                     member_since=forgery_py.date.date(True))
            db.session.add(u)
            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()

    def follow(self, user):
        if not self.is_following(user):
            f = Follow(follower=self, followed=user)
            db.session.add(f)
            db.session.commit()

    def unfollow(self, user):
        f = self.followed.filter_by(followed_id=user.id).first()
        if f:
            db.session.delete(f)
            db.session.commit()

    def is_following(self, user):
        """ 搜索指定被关注者用户，如果找到了就返回True """
        return self.followed.filter_by(followed_id=user.id).first() is not None

    def is_followed_by(self, user):
        """ 搜索指定关注者用户，如果找到了就返回True """
        return self.followers.filter_by(follower_id=user.id).first() is not None


"""
                                用户角色
    用户角色            权  限                        说  明
    匿名           0b00000000(0x00)        未登录的用户。在程序中只有阅读权限
    用户           0b00000111(0x07)        具有发布文章、发表评论和关注其他用户的权限。这是新用户的默认角色
    协管员         0b00001111(0x0f)        增加审查不当评论的权限
    管理员         0b11111111(0xff)        具有所有权限，包括修改其他用户所属角色的权限
"""


class Role(db.Model):
    """角色"""
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True)
    default = db.Column(db.Boolean, default=False, index=True)
    permissions = db.Column(db.Integer)
    users = db.relationship('User', backref='role', lazy='dynamic')

    def __repr__(self):
        return '<Role %r>' % self.name

    @staticmethod
    def insert_roles():
        roles = {
            'User': (Permission.FOLLOW |
                     Permission.COMMENT |
                     Permission.WRITE_ARTICLES, True),
            'Moderator': (Permission.FOLLOW |
                          Permission.COMMENT |
                          Permission.WRITE_ARTICLES |
                          Permission.MODERATE_COMMENTS, False),
            'Administrator': (0xff, False)
        }
        for r in roles:
            role = Role.query.filter_by(name=r).first()
            if role is None:
                role = Role(name=r)
            role.permissions = roles[r][0]
            role.default = roles[r][1]
            db.session.add(role)
        db.session.commit()


# 匿名用户类
class AnonymousUser(AnonymousUserMixin):

    def can(self, permissions):
        return False

    def is_administrator(self):
        return False


# 用户未登录时current_user值,方便检查用户是否登录
login_manager.anonymous_user = AnonymousUser


class Post(db.Model):
    """博客文章"""
    __tablename__ = 'posts'
    id = db.Column(db.Integer, primary_key=True)
    body = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, index=True, default=datetime.now)
    author_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    body_html = db.Column(db.Text)

    @staticmethod
    def generate_fake(count=100):
        """生成随机的博客文章"""
        from random import seed, randint
        import forgery_py

        seed()
        user_count = User.query.count()
        for i in range(count):
            u = User.query.offset(randint(0, user_count - 1)).first()
            p = Post(body=forgery_py.lorem_ipsum.sentences(randint(1, 3)),
                     timestamp=forgery_py.date.date(True),
                     author=u)
            db.session.add(p)
            db.session.commit()

    # linkify把url转换成a标签
    @staticmethod
    def on_changed_body(target, value, oldvalue, initiator):
        allowed_tags = ['a', 'abbr', 'acronym', 'b', 'blockquote', 'code','em', 'i', 'li',
                        'ol', 'pre', 'strong', 'ul', 'h1', 'h2', 'h3', 'p']
        target.body_html = bleach.linkify(bleach.clean(markdown(value, output_format='html'),
                                            tags=allowed_tags, strip=True))

db.event.listen(Post.body, 'set', Post.on_changed_body)


# Flask-Login 要求程序实现一个回调函数，使用指定的标识符加载用户
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))