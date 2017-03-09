# -*-coding:utf-8-*-

import json
import os

from tornado.gen import coroutine, Return
from tornado.web import RequestHandler

from lib.client import Kafka, SQLite3
from lib.config import instance
from lib.util import selectColumn, toTimestamp, md5Name

class KafkaHandler(RequestHandler):

    @coroutine
    def getTables(self, db, cluster):
        """ 返回指定数据库的所有表 """

        aliveTopics = instance.kafkaConfig[cluster]["topic"]

        with SQLite3(db) as client:
            tables = yield client.read("SELECT name FROM sqlite_master WHERE type='table'")

        raise Return([t[0] for t in tables if t[0] in aliveTopics])

    @coroutine
    def flowCounter(self, db, table, partitions, category, group=None):
        """ 计算实息流速 """

        if not group:
            # 计算cluster消息流速
            sql = """
                SELECT {0} 
                FROM `{1}` 
                WHERE category = '{2}' 
                    AND group_name = (
                        SELECT group_name 
                        FROM `{1}` 
                        ORDER BY id DESC 
                        LIMIT 1
                    ) ORDER BY id DESC 
                LIMIT 2
            """
        else:
            # 计算topic/group消息流速
            sql = """
                SELECT {} 
                FROM `{}` 
                WHERE category = '{}' 
                    AND group_name = '%s' 
                ORDER BY id DESC 
                LIMIT 2
            """ % group

        try:
            with SQLite3(db) as client:
                data = yield client.read(
                    sql.format(
                        ",".join(["p{0}".format(p) for p in partitions]), 
                        table, 
                        category
                    )
                )

            flow = sum(map(lambda x:x[0]-x[1], zip(*data)))

        except Exception:
            raise Return(0)

        raise Return(flow)

    @coroutine
    def getHighcharts(self, db, column, start, end, category, table, partitions, group=None, delta=False):
        """ 返回Hightchats数据 """

        chart, items = [], {}

        try:
            if group:
                sql = """
                    SELECT {0}, {1} 
                    FROM `{2}` 
                    WHERE category = '{3}' 
                        AND group_name = '%s' 
                        AND {0} BETWEEN {4} AND {5} 
                    GROUP BY {0}
                """ % group

            else:
                sql = """
                    SELECT {0}, {1} 
                    FROM `{2}` 
                    WHERE category = '{3}' 
                        AND group_name = (
                            SELECT group_name 
                            FROM `{2}`
                            ORDER BY id DESC 
                            LIMIT 1
                        ) 
                        AND {0} BETWEEN {4} AND {5} 
                    GROUP BY {0}
                """

            with SQLite3(db) as client:
                data = yield client.read(
                    sql.format(
                        column, 
                        ",".join(["avg(p{0})".format(p) for p in partitions]), 
                        table, 
                        category, 
                        start, end
                    )
                )
    
            meta = "timestamp, {}".format(",".join([ "{0}".format(p) for p in partitions ]))
            meta = tuple([x.strip() for x in meta.split(",")])
            data = [dict(zip(meta, x)) for x in data]

            for metric in data:
                for k, v in metric.items():
                    if k != "timestamp":
                        if not items.get(k):
                            items[k] = []
                        
                        items[k].append([ metric["timestamp"] * 1000, int(v) ])

            for name, data in items.items():
                chart.append({"name": "%02s" % name, "data": data})

            if delta:
                # 计算highchart图表delta数据
                metric = chart
                chart = []

                for item in metric:
                    data = item["data"]
                    index = range(len(data))

                    result = []
                    for i in index:
                        if i+1 <= index[-1]:
                            result.append([data[i+1][0], data[i+1][1] - data[i][1]])

                    chart.append({"name": item["name"], "data": result})

            chart = sorted(chart, key=lambda x:x["name"])

        except Exception:
            pass

        raise Return(chart)

    @coroutine
    def post(self):
        method = self.get_argument("method")

        if method == "jstree":
            jstree = []

            for cluster, metrics in instance.kafkaConfig.items():
                parent = {
                    "text": cluster, 
                    "children": [], 
                    "a_attr": {
                        "onclick": "kafkaClusterStat('%s')" % cluster 
                    }
                }

                topics = sorted(metrics["topic"].items(), key=lambda x:x[0])

                for topic, groups in topics:
                    children = {
                        "text": topic, 
                        "children": [], 
                        "a_attr": {
                            "onclick": "kafkaTopicStat('%s', '%s')" % (cluster, topic) 
                        }
                    }

                    groups = sorted(groups)

                    for group in groups:
                        children["children"].append(
                            {
                                "text": group, 
                                "icon": "/static/css/page.png", 
                                "a_attr": {
                                    "onclick": "kafkaGroupStat('%s', '%s', '%s')" % (cluster, topic, group) 
                                }
                            }
                        )

                    parent["children"].append(children)
                jstree.append(parent)

            self.write(json.dumps({"data": jstree}))

        if method in ["realtime", "topicList", "groupList", "chartList"]:
            cluster = self.get_argument("cluster")
            topic = self.get_argument("topic", None)
            group = self.get_argument("group", None)
            
            db = os.path.join(instance.kafkaDatadir, "kafka_{}.db".format(md5Name(cluster)))

            clusterConfig = instance.kafkaConfig[cluster]
            kafka = Kafka(clusterConfig['server'])
            
            if method == "realtime":
                # 获取Kafka实时数据
                inflow, outflow = 0, 0
                realtime = ""
    
                if not topic:
                    # 统计kafka集群消息流入流出
                    tables = yield self.getTables(db, cluster)

                    for table in tables:
                        partitions = yield kafka.getPartition(table)
                        value = yield self.flowCounter(db, table, partitions, "logsize")
                        inflow += value

                        for group in clusterConfig["topic"][table]:
                            value = yield self.flowCounter(db, table, partitions, "offsets", group)
                            outflow += value
    
                elif topic:
                    partitions = yield kafka.getPartition(topic)
                    logsize = yield kafka.getLogsize(topic, partitions)
                    logsize = sum(logsize.values())
    
                    realtime = realtime + """
                        <span class="label label-success">Logsize</span> {}
                    """.format(logsize)
    
                    if group:
                        offsets = yield kafka.getOffsets(topic, partitions, group)
                        offsets = sum(offsets.values())

                        lag = logsize - offsets
                        lag = (lag > 0 and [lag] or [0])[0]
    
                        realtime = realtime + """
                            <span class="label label-success">Offsets</span> {}
                            <span class="label label-danger">Lag</span> {}
                        """.format(offsets, lag)
    
                        value = yield self.flowCounter(db, topic, partitions, "offsets", group)
                        outflow += value

                    else:
                        for group in clusterConfig["topic"][topic]:
                            value = yield self.flowCounter(db, topic, partitions, "offsets", group)
                            outflow += value

                    value = yield self.flowCounter(db, topic, partitions, "logsize")
                    inflow += value

                realtime = """
                    <span class="label label-success">流入消息</span> {}
                    <span class="label label-success">流出消息</span> {}
                """.format(inflow, outflow) + realtime
    
                self.write(json.dumps({"realtime": realtime}))
    
            if method == "topicList":
                # 获取指定Kafka集群下TopicLogsize排名
    
                topicList = []
                tables = yield self.getTables(db, cluster)
        
                for table in tables:
                    partitions = yield kafka.getPartition(table)
                    value = yield self.flowCounter(db, table, partitions, "logsize")

                    topicList.append({
                        "topic": table,
                        "logsize": value
                    })
    
                self.write(json.dumps({"data": topicList}))
    
            if method == "groupList":
                # 获取指定Topic下ConsumerGroupLag排名
    
                groupList = []
                partitions = yield kafka.getPartition(topic)
    
                for group in clusterConfig["topic"][topic]:
                    logsize = yield kafka.getLogsize(topic, partitions)
                    offsets = yield kafka.getOffsets(topic, partitions, group)
                    logsize = sum(logsize.values())
                    offsets = sum(offsets.values())
                    lag = logsize - offsets
                    groupList.append({'group': group, 'lag': (lag > 0 and [lag] or [0])[0]})
                    
                self.write(json.dumps({"data": groupList}))
    
            if method == "chartList":
                # 获取指定consumerGroup hightcharts数据
                # 包含增长数据与delta数据
                
                start = toTimestamp(self.get_argument("start"))
                end = toTimestamp(self.get_argument("end"))
                partitions = yield kafka.getPartition(topic)
                column = selectColumn(end - start)
    
                # 生成Hightcharts图表
                logsize, offsets, lag, logsizeDelta, offsetsDelta = [], [], [], [], []

                if not group:
                    logsize = yield self.getHighcharts(db, column, start, end, "logsize", topic, partitions)
                    logsizeDelta = yield self.getHighcharts(db, column, start, end, "logsize", topic, partitions, delta=True)
        
                else:
                    lag = yield self.getHighcharts(db, column, start, end, "lag", topic, partitions, group)
                    offsets = yield self.getHighcharts(db, column, start, end, "offsets", topic, partitions, group)
                    offsetsDelta = yield self.getHighcharts(db, column, start, end, "offsets", topic, partitions, group, delta=True)

                self.write(json.dumps({"logsize": logsize, "offsets": offsets, "lag": lag, "logsizeDelta": logsizeDelta, "offsetsDelta": offsetsDelta}))

