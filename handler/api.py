# -*-coding:utf-8-*-

import os
import json

from tornado.gen import coroutine, Return
from tornado.web import RequestHandler

from lib.client import Kafka, Zookeeper
from lib.config import instance

class ApiHandler(RequestHandler):

    @coroutine
    def get(self, uri):
        """ 调用Api返回Kafka Zookeeper数据 """

        cluster = self.get_argument("cluster")
        
        if uri == "kafka":
            topic = self.get_argument("topic")
            group = self.get_argument("group")

            # 检测api参数可用性
            if (not instance.kafkaConfig.get(cluster)) or (not instance.kafkaConfig[cluster]["topic"].get(topic)) or (group not in instance.kafkaConfig[cluster]["topic"][topic]):
                raise Return()

            try:
                # 从缓存中获取最近一次保存的数据
                logsize = instance.kafkaCache[cluster][topic]["logsize"]
                offsets = instance.kafkaCache[cluster][topic]["group"][group]["offsets"]
                lag = instance.kafkaCache[cluster][topic]["group"][group]["lag"]
            
            except KeyError:
                # 调用kafka客户端获取最新数据
                kafka = Kafka(instance.kafkaConfig[cluster]["server"])
                partitions = yield kafka.getPartition(topic)
                logsize = yield kafka.getLogsize(topic, partitions)
                offsets = yield kafka.getOffsets(topic, partitions, group)
                lag = {p: logsize[p] - offsets[p] for p in partitions}

            logsize = sum(logsize.values())
            offsets = sum(offsets.values())
            lag = sum(lag.values()) if sum(lag.values()) > 0 else 0

            self.write("%s=%s %s=%s %s=%s" % ("logsize", logsize, "offsets", offsets, "lag", lag))

        elif uri == "zookeeper":
            server = self.get_argument("server")

            # 检测api参数可用性
            if (not instance.zookeeperConfig.get(cluster)) or (not [x for x in instance.zookeeperConfig[cluster]["server"] if server in x]):
                    raise Return()

            try:
                # 从缓存中获取最近一次保存的数据
                metrics = instance.zookeeperCache[server]

            except KeyError:
                # 调用zookeeper客户端获取最新mntr数据
                with Zookeeper(server) as zk:
                    metrics = yield zk.fetchMntr()

            data = ["%s=%s" % (x, metrics[x]) for x in metrics.keys() if x != "zk_version"]
            
            self.write(" ".join(data))

