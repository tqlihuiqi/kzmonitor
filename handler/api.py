# -*-coding:utf-8-*-

import os
import json

from tornado.gen import coroutine, Return
from tornado.web import RequestHandler

from lib.client import SQLite3
from lib.config import instance
from lib.util import md5Name

class ApiHandler(RequestHandler):

    @coroutine
    def get(self, uri):
        """ 调用Api返回Kafka Zookeeper数据 """

        cluster = self.get_argument("cluster")
        result = []
        
        if uri == "kafka":
            db = os.path.join(instance.kafkaDatadir, "kafka_{}.db".format(md5Name(cluster)))
            topic = self.get_argument("topic")
            group = self.get_argument("group")
            
            if not os.path.isfile(db):
                raise Return()

            try:
                with SQLite3(db) as client:
                    # 获取Table Schema
                    schema = yield client.read("PRAGMA table_info(`{}`)".format(topic))
                    column = ",".join([ x[1] for x in schema if x[1][0] == "p" ])
    
                    for category in ["logsize", "offsets", "lag"]:
                        sql = """
                            SELECT category, {0} 
                            FROM `{1}`
                            WHERE id = (
                                SELECT max(id) 
                                FROM `{1}`
                                WHERE category = '{2}'
                                AND group_name = '{3}'
                            )
                        """.format(column, topic, category, group)
        
                        data = yield client.read(sql)
                        result.append("%s=%s" % (data[0][0],sum(data[0][1:])))
            except:
                pass

        elif uri == "zookeeper":
            db = os.path.join(instance.zookeeperDatadir, "zookeeper_{}.db".format(md5Name(cluster)))
            server = self.get_argument("server")

            if not os.path.isfile(db):
                raise Return()

            try:
                with SQLite3(db) as client:
                    meta = (
                        "activeClients",
                        "avgLatency",
                        "maxLatency",
                        "outstandingRequest",
                        "sentPacket",
                        "receivePacket",
                        "watchCount",
                        "znodeCount",
                        "dataSize",
                        "openFileCount"
                    )

                    data = yield client.read(
                        """
                            SELECT alive_clients,
                                   avg_latency,
                                   max_latency,
                                   out_requests,
                                   sent_packets,
                                   receive_packets,
                                   watch_count,
                                   znode_count,
                                   data_size,
                                   open_file_count
                            FROM `zkstatus`
                            WHERE id = (
                                SELECT max(id)
                                FROM `zkstatus`
                                WHERE ip = '{}'
                            )
                        """.format(server)
                    )
                    
                    for i, v in enumerate(data[0]):
                        result.append("%s=%s" % (meta[i], v))
            except:
                pass

        self.write(" ".join(result))
