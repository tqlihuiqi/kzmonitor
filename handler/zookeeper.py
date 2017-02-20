# -*-coding:utf-8-*-

import json
import os

from tornado.gen import coroutine, Return
from tornado.web import RequestHandler

from lib.client import SQLite3, Zookeeper
from lib.config import instance
from lib.util import selectColumn, toTimestamp, md5Name

class ZookeeperHandler(RequestHandler):

    @coroutine
    def getHighcharts(self, cluster, start, end):
        """ 返回Hightchats数据 """

        column = selectColumn(end - start)
        db = os.path.join(instance.zookeeperDatadir, "zookeeper_{}.db".format(md5Name(cluster)))

        charts = []

        try:
            sql = """
                SELECT 
                    ip,
                    {0},
                    avg(alive_clients),
                    avg(receive_packets),
                    avg(sent_packets),
                    avg(out_requests),
                    avg(watch_count),
                    avg(znode_count),
                    avg(data_size),
                    avg(open_file_count),
                    avg(min_latency),
                    avg(avg_latency),
                    avg(max_latency)
                FROM `{1}`
                WHERE {0} BETWEEN {2} AND {3}
                GROUP BY {0}, ip
            """.format(column, "zkstatus", start, end)
  
            with SQLite3(db) as client:
                data = yield client.read(sql)

            meta = ("ip", "timestamp", "aliveClient", "recvPkt", "sentPkt", "outRest", "watchCount", "znodeCount", "dataSize", "openFileCount", "minLatency", "avgLatency", "maxLatency")
            data = [dict(zip(meta, x)) for x in data]

            for k in meta:
                if k not in ["ip", "timestamp"]:
                    chart, items = [], {}

                    for metric in data:
                        ip = metric["ip"]
    
                        if not items.get(ip):
                            items[ip] = []
    
                        items[ip].append([ metric["timestamp"]*1000, int(metric[k]) ])

                    for k, v in items.items():
                        chart.append({"name": k, "data": v})

                    charts.append(chart)
    
        except Exception:
            pass

        raise Return(charts)

    @coroutine
    def post(self):
        method = self.get_argument("method")

        if method == "jstree":
            jstree = []

            for cluster, nodes in instance.zookeeperConfig.items():
                parent = {
                    "text": cluster, 
                    "children": [], 
                    "a_attr": {
                        "onclick": "zkClusterStat('%s')" % cluster 
                    }
                }

                for node in nodes["server"]:
                    parent["children"].append(
                        {
                            "text": node, 
                            "icon": "/static/css/page.png", 
                            "a_attr": {
                                "onclick": "zkServerStat('%s')" % node 
                            }
                        }
                    )

                parent["children"].append(
                    { 
                        "children": [], 
                        "a_attr": {
                            "onclick": "zkGetNode(this.id, '%s', '/')" % node 
                        }, 
                        "state": {
                            "opened": True
                        },
                        "text": "/"
                    }
                )

                jstree.append(parent)

            self.write(json.dumps({"data": jstree}))

        if method == "serverStat":
            cluster = self.get_argument("cluster")
            start = toTimestamp(self.get_argument("start"))
            end = toTimestamp(self.get_argument("end"))

            # 获取mnrt higharts图表数据
            charts = yield self.getHighcharts(cluster, start, end)

            if not charts:
                raise Return(self.write(json.dumps(charts)))

            aliveClient, recvPkt, sentPkt, outRest, watchCount, znodeCount, dataSize, openFileCount, minLatency, avgLatency, maxLatency = charts

            self.write(json.dumps(
                {
                    "aliveClient": aliveClient,
                    "recvPkt": recvPkt,
                    "sentPkt": sentPkt,
                    "outRest": outRest,
                    "watchCount": watchCount,
                    "znodeCount": znodeCount,
                    "dataSize": dataSize,
                    "openFileCount": openFileCount,
                    "minLatency": minLatency,
                    "avgLatency": avgLatency,
                    "maxLatency": maxLatency
                }
            ))

        if method in ["getNode", "getStat", "getClient", "getData"]:
            server = self.get_argument("server", None)

            with Zookeeper(server) as client:

                if method == "getNode":
                    # 获取指定node中的资源

                    path = self.get_argument("path")
                    children = yield client.getChildren(path)

                    trees = []
                    for child in children:
                        _, stat = yield client.get(os.path.join(path, child))
                    
                        trees.append({
                            'name': child,
                            'path': os.path.join(path, child),
                            'leaf': (stat['numChildren'] and [True] or [False])[0]
                        })
            
                    self.write(json.dumps(sorted(trees, key=lambda x:x['name'])))

                if method == "getStat":
                    # 获取指定server的mntr数据

                    mntr = yield client.fetchMntr()

                    realtime = """
                        <span class="label label-danger">运行模式</span> {}
                        <span class="label label-danger">运行版本</span> {}
                        <br><br>
                        <span class="label label-success">活跃客户端</span> {}
                        <span class="label label-success">最小延迟</span> {}
                        <span class="label label-success">平均延迟</span> {}
                        <span class="label label-success">最大延迟</span> {}
                        <span class="label label-success">挂起的请求</span> {}
                        <span class="label label-success">发送数据包</span> {}
                        <span class="label label-success">接收数据包</span> {}
                        <br><br>
                        <span class="label label-primary">watch数量</span> {}
                        <span class="label label-primary">znode数量</span> {}
                        <span class="label label-primary">打开的文件数量</span> {}
                        <span class="label label-primary">数据大小</span> {}
                    """.format(
                        mntr["zk_server_state"],
                        mntr["zk_version"],
                        mntr["zk_num_alive_connections"],
                        mntr["zk_min_latency"],
                        mntr["zk_avg_latency"],
                        mntr["zk_max_latency"],
                        mntr["zk_outstanding_requests"],
                        mntr["zk_packets_sent"],
                        mntr["zk_packets_received"],
                        mntr["zk_watch_count"],
                        mntr["zk_znode_count"],
                        mntr["zk_open_file_descriptor_count"],
                        mntr["zk_approximate_data_size"]
                    )

                    self.write(json.dumps({"realtime": realtime}))

                if method == "getClient":
                    # 获取指定server的client列表

                    cons = yield client.fetchCons()
                    self.write(json.dumps({"data": cons}))

                if method == "getData":
                    # 获取指定znode节点资源

                    path = self.get_argument("path")
                    data, stat = yield client.get(path)

                    if not data:
                        data = None

                    elif type(data) == str:
                        data = data.encode("utf8").decode("utf8")

                    elif type(data) == bytes:
                        data = data.decode("utf8")

                    self.write(json.dumps({"data": data, "stat": stat}))
