# -*-coding:utf-8-*-

import sys
import os

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.web import Application, RequestHandler

basedir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, basedir)

from handler.index import IndexHandler
from handler.kafka import KafkaHandler
from handler.zookeeper import ZookeeperHandler
from lib.config import instance
from lib.writer import KafkaWriter, ZookeeperWriter

class KZMonitor(Application):

    def __init__(self):
        handlers = [
            (r'/', IndexHandler),
            (r'/kafka', KafkaHandler),
            (r'/zookeeper', ZookeeperHandler),
        ]

        settings = dict(
            static_path = os.path.join(basedir, 'static'),
            template_path = os.path.join(basedir, 'template')   
        )

        super(KZMonitor, self).__init__(handlers, **settings)

    def worker(self):
        """ 启动工作线程进行数据采集入库 """

        kw = KafkaWriter()
        PeriodicCallback(kw.write, instance.kafkaPeriod).start()

        zw = ZookeeperWriter()
        PeriodicCallback(zw.write, instance.zookeeperPeriod).start()

if __name__ == '__main__':
    app = KZMonitor()
    app.worker()
    server = HTTPServer(app)

    server.listen(
        instance.cf.getint('server', 'listen.port'), 
        instance.cf.get('server', 'listen.ip')
    )
    
    IOLoop.instance().start()
