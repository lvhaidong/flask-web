# -*- coding: utf-8 -*-
import os

"""
    1. --parallel-mode：使Coverage监测被测代码子进程的覆盖率，如果被测代码是多进程的，必须使用此参数；
    2. --branch：统计分支代码覆盖率，加上这个参数可使统计更精确,
         branch=True 选项开启分支覆盖分析
    3.--include: 限定要统计代码的路径，如果不限定，Coverage会把请求涉及到的所有代码，
                 包括系统库和Tornado框架的代码都分析一遍，会大大拉低代码覆盖率；
        include 选项用来限制程序包中文件的分析范围，只对这些文件中的代码进行覆盖 检测。
"""
COV = None
if os.environ.get('FLASK_COVERAGE'):
    import coverage
    # 启动覆盖检测引擎
    COV = coverage.coverage(branch=True, include='app/*')
    COV.start()

from app import create_app, db
from app.models import User, Role
from flask_script import Manager, Shell
from flask_migrate import Migrate, MigrateCommand

env = os.getenv('FLASK_CONFIG') or 'default'
app = create_app(env)
manager = Manager(app)
migrate = Migrate(app, db)


def make_shell_context():
    return dict(app=app, db=db, User=User, Role=Role)
manager.add_command("shell", Shell(make_context=make_shell_context))
manager.add_command('db', MigrateCommand)


@manager.command
def test(coverage=False):
    """Run the unit tests."""
    if coverage and not os.environ.get('FLASK_COVERAGE'):
        import sys
        os.environ['FLASK_COVERAGE'] = '1'
        os.execvp(sys.executable, [sys.executable] + sys.argv)
    import unittest
    tests = unittest.TestLoader().discover('tests')
    unittest.TextTestRunner(verbosity=2).run(tests)
    if COV:
        COV.stop()
        COV.save()
        print('Coverage Summary:')
        COV.report()
        basedir = os.path.abspath(os.path.dirname(__file__))
        covdir = os.path.join(basedir, 'tmp/coverage')
        COV.html_report(directory=covdir)
        print('HTML version: file://%s/index.html' % covdir)
        COV.erase()

if __name__ == '__main__':
    manager.run()

