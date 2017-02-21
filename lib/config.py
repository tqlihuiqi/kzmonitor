# -*-coding:utf-8-*-

import os
import yaml

try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

class ConfigInstance(object):

    def __init__(self):
        self.basedir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
        
        self.cf = ConfigParser()
        self.cf.read(os.path.join(self.basedir, "etc/server.conf"))

        # 缓存
        self.kafkaCache = dict()
        self.zookeeperCache = dict()

        # 配置文件项
        self.kafkaPeriod = self.cf.getint("server", "kafka.fetch.period.ms")
        self.kafkaDataExpire = self.cf.getint("server", "kafka.data.expire.day")
        self.kafkaDatadir = self.cf.get("server", "kafka.data.dir")
        self.zookeeperPeriod = self.cf.getint("server", "zookeeper.fetch.period.ms")
        self.zookeeperDataExpire = self.cf.getint("server", "zookeeper.data.expire.day")
        self.zookeeperDatadir = self.cf.get("server", "zookeeper.data.dir")

        with open(os.path.join(self.basedir, "etc/kafka.yaml")) as k, open(os.path.join(self.basedir, "etc/zookeeper.yaml")) as z:
            self.kafkaConfig = yaml.load(k)
            self.zookeeperConfig = yaml.load(z)

instance = ConfigInstance()