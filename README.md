
# Kafka Zookeeper Monitor

## 功能

### Kafka

* In/Out消息统计
* Topic增量数据排名
* Consumer延迟实时排名
* 可查看每个Topic Partition的Logsize Offsets Lag数据
* 实时获取指定Group的Logsize Offsets Lag
* 可定制的历史数据存储时间

### Zookeeper

* 集群各个节点性能数据
* 单个Server性能数据与该Server的当前活跃客户端
* 展示指定znode数据(zookeeper get操作)
* 可定制的历史数据存储时间

## 安装

```shell
cd kzmonitor; pip install -r requirements.txt
```

## 配置文件

* `etc/server.conf` 控制数据采集间隔与存储时间等
* `etc/kafka.yaml` 监控Kafka集群配置
* `etc/zookeeper.yaml` 监控Zookeeper集群配置

## 启动

```shell
cd kzmonitor; nohup python kzmonitor.py &> /dev/null &
```

