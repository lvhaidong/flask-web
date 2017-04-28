# -*- coding: utf-8 -*-
import logging, os


class Logger(object):
    def __init__(self, path, clevel=logging.DEBUG, flevel=logging.DEBUG):
        self.logger = logging.getLogger(path)
        self.logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
        # 设置CMD日志
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(clevel)

        # 设置文件日志
        fh = logging.FileHandler(path)
        fh.setFormatter(fmt)
        fh.setLevel(flevel)

        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

if __name__ =='__main__':
    # clevel 终端显示bug,文件日志显示bug
    logyyx = Logger('yyx.log', clevel=logging.ERROR, flevel=logging.INFO)
    logyyx.debug('一个debug信息')
    logyyx.info('一个info信息')
    logyyx.warning('一个warning信息')
    logyyx.error('一个error信息')
    logyyx.critical('一个致命critical信息')