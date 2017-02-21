# -*-coding:utf-8-*-

import json
import re
import time

from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor

from kafka import KafkaClient, KafkaConsumer
from kafka.common import TopicPartition, OffsetRequestPayload
from kafka.errors import UnknownTopicOrPartitionError
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from sqlalchemy import create_engine

class SQLite3(object):
    executor = ThreadPoolExecutor(20)

    def __init__(self, db):
        self.db = create_engine('sqlite:///'+db, connect_args={'check_same_thread': False}, strategy = 'threadlocal')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.db.dispose()

    @run_on_executor
    def read(self, sql):
        """ 查询数据 """

        retry = 0

        while retry <= 5:
            try:
                cursor = self.db.execute(sql)
                break   
            except Exception as e:
                if "database is locked" in str(e):
                    time.sleep(1)
                
                retry += 1

        data = cursor.fetchall()
        cursor.close()
        return data

    @run_on_executor
    def write(self, sql):
        """ 写数据 """
        
        self.db.begin()
        try:
            cursor = self.db.execute(sql)
            cursor.close()
        except:
            raise
        else:
            self.db.commit()

    @run_on_executor
    def vacuum(self):
        """ 整理数据库碎片 """

        self.db.execute("VACUUM")

    @run_on_executor
    def clean(self, table, expireTime):
        """ 清理过期数据 """

        self.db.execute("DELETE FROM `{}` WHERE timestamp < {}".format(table, expireTime))

    @run_on_executor
    def createKafkaTable(self, topic, partitions, recreate=False):
        """ 按照Kafka Topic创建对应的SQLite表 """

        if recreate:
            try:
                self.db.execute("DROP TABLE `{}`".format(topic))
            except Exception:
                pass
    
        self.db.execute(
            """
                CREATE TABLE IF NOT EXISTS `{}` (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_name VARCHAR(100) NOT NULL,
                    category VARCHAR(8) NOT NULL,
                    timestamp INTEGER NOT NULL,
                    timestamp5m INTEGER NOT NULL,
                    timestamp10m INTEGER NOT NULL,
                    timestamp1h INTEGER NOT NULL,
                    {}
                )
            """.format(topic, ",".join([ "p{} bigint NOT NULL".format(p) for p in partitions ]))
        )
    
        # 创建索引
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_group ON `{}` (group_name)".format(topic))
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_category ON `{}` (category)".format(topic))
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON `{}` (timestamp)".format(topic))
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_timestamp5m ON `{}` (timestamp5m)".format(topic))
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_timestamp10m ON `{}` (timestamp10m)".format(topic))
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_timestamp1h ON `{}` (timestamp1h)".format(topic))

    @run_on_executor
    def createZkTable(self, table):
        """ 创建Zookeeper数据表 """

        self.db.execute(
            """
                CREATE TABLE IF NOT EXISTS `{}` (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ip VARCHAR(15) NOT NULL,
                    alive_clients INTEGER NOT NULL,
                    receive_packets INTEGER NOT NULL,
                    sent_packets INTEGER NOT NULL,
                    out_requests INTEGER NOT NULL,
                    watch_count INTEGER NOT NULL,
                    znode_count INTEGER NOT NULL,
                    data_size INTEGER NOT NULL,
                    open_file_count INTEGER NOT NULL,
                    min_latency INTEGER NOT NULL,
                    avg_latency INTEGER NOT NULL,
                    max_latency INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    timestamp5m INTEGER NOT NULL,
                    timestamp10m INTEGER NOT NULL,
                    timestamp1h INTEGER NOT NULL
                )
            """.format(table)
        )
        
        # 创建索引
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON `{}` (timestamp)".format(table))
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_timestamp5m ON `{}` (timestamp5m)".format(table))
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_timestamp10m ON `{}` (timestamp10m)".format(table))
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_timestamp1h ON `{}` (timestamp1h)".format(table))

class Kafka(object):
    executor = ThreadPoolExecutor(20)
    
    def __init__(self, broker):
        self.broker = broker
        self.client = KafkaClient(broker, timeout=3)

    @run_on_executor
    def getPartition(self, topic):
        """ 指定topic返回partition列表 """

        return self.client.get_partition_ids_for_topic(topic)

    @run_on_executor
    def getLogsize(self, topic, partitions):
        """ 指定topic与partition列表, 返回logsize数据 """

        tp = self.client.send_offset_request([OffsetRequestPayload(topic, p, -1, 1) for p in partitions])
        return {p.partition: p.offsets[0] for p in tp}

    @run_on_executor
    def getOffsets(self, topic, partitions, group):
        """ 指定topic、partition和group, 返回offsets数据 """

        try:
            # 尝试使用zookeeper-storage api获取offsets数据
            # 未获得指定group的offsets数据将抛出UnknownTopicOrPartitionError异常
            tp = self.client.send_offset_fetch_request(group, [OffsetRequestPayload(topic, p, -1, 1) for p in partitions])
            offsets = {p.partition: p.offset for p in tp}

        except UnknownTopicOrPartitionError:
            # 收到异常后使用kafka-storage api获取offsets数据
            consumer = KafkaConsumer(group_id=group, bootstrap_servers=self.broker, enable_auto_commit=False)
            tp = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tp)
            offsets = {p.partition: consumer.position(p) for p in tp}

        return offsets

class Zookeeper(object):
    executor = ThreadPoolExecutor(20)

    def __init__(self, server):
        self.client = KazooClient(hosts=server, timeout=3, read_only=True, max_retries=1)
        self.client.start(timeout=3)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.client.stop()
        self.client.close()

    @run_on_executor
    def fetchCons(self):
        """ 返回活跃的客户端状态数据 """

        name = ('ip', 'queued', 'port', 'recved', 'sent', 'sid', 'lop', 'est', 'to', 'lcxid', 'lzxid', 'lresp', 'llat', 'minlat', 'avglat', 'maxlat')
        pattern = re.compile(r'/(.+):(\d+).+queued=(\d+).+recved=(\d+).+sent=(\d+).+sid=(\w+).+lop=(\w+).+est=(\d+).+to=(\d+).+lcxid=(\w+).+lzxid=(\w+).+lresp=(\d+).+llat=(\d+).+minlat=(\d+).+avglat=(\d+).+maxlat=(\d+)')

        data = []
        for row in self.client.command(cmd=b'cons').split('\n'):
            if row and ')' == row[-1]:
                metric = pattern.findall(row)

                if metric:
                    data.append(dict(zip(name, metric[0])))

        return data

    @run_on_executor
    def fetchMntr(self):
        """ 返回zk节点状态数据 """

        data = dict(
            item.split('\t', 1)
            for item in self.client.command(b'mntr').split('\n')[:-1]
        )
        data.update({'alive': (True if self.client.command(cmd=b'ruok')=='imok' else False)})
        return data

    @run_on_executor
    def get(self, path):
        """ 返回指定znode的状态与数据 """

        data, stat = self.client.get(path)
        return data, json.loads(json.dumps(stat._asdict()))

    @run_on_executor
    def getChildren(self, path):
        """ 返回指定zk目录中的资源列表 """

        try:
            children = map(str, self.client.get_children(path))
        except NoNodeError as e:
            return []

        return children

