# -*- coding: utf-8 -*-
__author__ = "lvhaidong"
from functools import wraps
from flask import abort
from flask_login import current_user
from app.models import Permission


"""
    如果用户是否具有的指定权限，则返回403错误码，即 HTTP“禁止”错误
"""


def permission_required(permission):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.can(permission):
                abort(403)
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def admin_required(f):
    return permission_required(Permission.ADMINISTER)(f)