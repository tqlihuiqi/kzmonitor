# -*-coding:utf-8-*-

import os
import time

from tornado.gen import coroutine

from .client import Kafka, Zookeeper, SQLite3
from .config import instance
from .util import timeColumn, md5Name

class KafkaWriter(object):

    @coroutine
    def write(self):
        """
        后台任务连接kafka各个集群
        获取所有topicGroup的 logsize offsets lag数据
        将各group的数据写入存储中
        删除过期数据, 由参数kafka.data.expire.day控制
        """

        for cluster, metrics in instance.kafkaConfig.items():
            kafka = Kafka(metrics["server"])
            
            if not os.path.exists(instance.kafkaDatadir):
                raise IOError(instance.kafkaDatadir + ": No such file or directory.")

            with SQLite3(os.path.join(instance.kafkaDatadir, "kafka_{}.db".format(md5Name(cluster)))) as client:
                # 整理数据文件碎片
                yield client.vacuum()
            
                for topic, groups in metrics["topic"].items():
                    partitions = yield kafka.getPartition(topic)
                    # 创建表
                    yield client.createKafkaTable(topic, partitions)
    
                    sql = """
                        INSERT INTO `{}` (group_name, category, {}, timestamp, timestamp5m, timestamp10m, timestamp1h)
                        VALUES %s
                    """.format(topic, ",".join([ "p{}".format(p) for p in partitions ]))
                    
                    logsize = yield kafka.getLogsize(topic, partitions)
                    values = []
                    
                    for group in groups:
                        offsets = yield kafka.getOffsets(topic, partitions, group)
                        lag = {p: logsize[p] - offsets[p] for p in partitions}
                    
                        for category, metrics in dict(logsize=logsize, offsets=offsets, lag=lag).items():
                            partitionValues = ", ".join(map(str,[ metrics.get(p) for p in partitions ]))
                            timestamp, timestamp5m, timestamp10m, timestamp1h = timeColumn(int(time.time()))
                            values.append("('{}', '{}', {}, {}, {}, {}, {})".format(group, category, partitionValues, timestamp, timestamp5m, timestamp10m, timestamp1h))
                    
                    # 批量写入数据
                    # 增加Partition时将重建表
                    try:
                        yield client.write(sql % ", ".join(values))
                        # 清理过期数据
                        yield client.clean(topic, int(time.time()) - int(instance.kafkaDataExpire)*86400)
                   
                    except Exception:
                        yield client.createKafkaTable(topic, partitions, True)

class ZookeeperWriter(object):

    @coroutine
    def write(self):
        """
        后台任务连接各Zookeeper集群的各节点
        收集各节点的mntr节点数据, 将数据写入存储中
        删除过期数据, 由参数zookeeper.data.expire.day控制
        """

        for cluster, metrics in instance.zookeeperConfig.items():
            if not os.path.exists(instance.zookeeperDatadir):
                raise IOError(instance.zookeeperDatadir + ": No such file or directory.")

            with SQLite3(os.path.join(instance.zookeeperDatadir, "zookeeper_{}.db".format(md5Name(cluster)))) as client:
                # 整理数据文件碎片
                yield client.vacuum()
                # 创建表
                yield client.createZkTable("zkstatus")

                for ip in metrics["server"]:
                    with Zookeeper(ip) as zk:
                        mntr = yield zk.fetchMntr()
                        timestamp, timestamp5m, timestamp10m, timestamp1h = timeColumn(int(time.time()))

                        sql = """
                            INSERT INTO `zkstatus` (ip, alive_clients, receive_packets, sent_packets, out_requests, watch_count, znode_count, data_size, open_file_count, min_latency, avg_latency, max_latency, timestamp, timestamp5m, timestamp10m, timestamp1h) 
                            VALUES ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                        """.format(
                            ip.split(":")[0],
                            mntr["zk_num_alive_connections"],
                            mntr["zk_packets_received"],
                            mntr["zk_packets_sent"],
                            mntr["zk_outstanding_requests"],
                            mntr["zk_watch_count"],
                            mntr["zk_znode_count"],
                            mntr["zk_approximate_data_size"],
                            mntr["zk_open_file_descriptor_count"],
                            mntr["zk_min_latency"],
                            mntr["zk_avg_latency"],
                            mntr["zk_max_latency"],
                            timestamp,
                            timestamp5m,
                            timestamp10m, 
                            timestamp1h
                        )
                        yield client.write(sql)
                        # 清理过期数据
                        yield client.clean("zkstatus", int(time.time()) - int(instance.zookeeperDataExpire)*86400)

