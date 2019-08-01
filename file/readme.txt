JDK 1.7

1. 介绍 公司大数据中使用Mongo作为数据存储工具，目标集群使用一主三从，因为数据量较大，在使用的过程中从库查询繁忙时会导致主从同步慢，出现数据延迟，为了解决这个问题，就自己开发了Mongo数据同步程序，同时该同步程序支持数据目标动态扩展，可自有定制自己的数据目标实现类。

目前支持的源端Mongo版本为 2.x 3.x
目前支持的目标为mongo和kafka

2. 工作原理
 mongo日志集合：副本集 oplog.rs ； 主从 oplog.$main；为固定大小的集合，数据采用先入先删的方式，当集合满时会删除先插入的日志信息。（注意：当日志量很大时，需要控制要集合大小，防止日志时间过短导致数据来不急同步就丢失的问题。）
 从mongo的oplog集合中获取日志信息，通过ringbuffer队列进行数据分发，根据配置将日志信息分发到不同的目标处理器。

3. 配置信息
支持本地配置和zookeeper配置2种方式
本地配置：
config.json

{
  strConfigMode:"local",    # 配置信息标记， local 本地配置； zk zookeeper配置
  strZKClientDomain:"",     # zk 配置时对应的服务器
  strRootNodeName:"",       # zk 配置时的根节点，进程节点需要按照进程名称创建

  strEndLogPosPath:"D://M2M4J//EndLogPos//",  # 位置信息保存目录
  
  nMoniterState: 1,   # 是否启用监控线程
  nJettyPort: 1214,   # 监控服务端口（使用jetty创建web容器）

  nCuckoo: 1,                  # 异常是否进行通知
  strCuckooJSON: "email",      # 使用email进行通知，可以自由扩展通知方式（钉钉、短信）

  sourceConfig:[               # mongo源配置，支持多源配置
    {
      strWorkerId:"21111",     # 源标记
      nDebug:0,                # 是否启用debug模式
      strDBIP:"",              # mongo服务ip
      nDBPort:,                # mongo服务端口
      strUserName:"",          # 帐号
      strPassWord:"",          # 密码
      nVersion:3,              # mongo版本  2: 2.x ; 3: 3.x
      strOplogModel:"rs",      # mongo集群方式（rs 副本集；admin 主从）
      arrOplogEventFilter:["i","u","d"],          # 日志事件过滤（i 插入 u 修改 d 删除）
      arrOplogDataFilter:["MonitorGather.*"],     # 日志库、集合过滤，支持正则表达式
      nPositionEnable:0,                          # 是否使用配置的位置作为开始位置
      nTime:-1,                                   # 时间（unix时间戳）
      nIncrement:-1,                              # 序列
      strDMLListenerJSON:"/target-config.json"    # 目标配置
    }
  ]
}

target-config.json

# 支持多目标配置，多目标订阅同一个源日志信息
[
  {
    strSign: "test",                    # 目标标记
    strDMLTargetClass: "oplog.listener.mongo.impl.DMLMongoListener",        # 目标Mongo实现类
    strMongoIP: "",                     # 目标Mongo ip
    nMongoPort: ,                       # 目标Mongo端口
    nVersion: 2,                        # 目标Mongo版本
    strMongoDBName: "",                 # 目标库
    strMongoUserName: "",               # 目标帐号
    strMongoPassWord: "",               # 目标密码
    jsonDBRule:{                        # 目标库转换方式（将日志中的Moni库转换为目标中的Acc库）
      "Moni": "Acc"
    },
    jsonTableRule:{                     # 目标表转换方式（将日志中的Moni表转换为目标中的Acc表）
      "Moni": "Acc"
    }
  },
  {
    strSign: "test2",                   # 目标标记
    strDMLTargetClass: "oplog.listener.kafka.impl.DMLKafkaListener",          # 目标Kafka实现类
    jsonParamObject: {                  # Kafka 配置信息
      "bootstrap.servers": "",          
      "acks": "all",
      "retries": 3,
      "batch.size": 16384,
      "max.request.size": 10485760,
      "send.buffer.bytes": 10485760
    }
  }
]


4. 位置信息保存方式
在配置 strEndLogPosPath 目录下，按照 源id+源端口+源标记 创建目录，按照目标标记 strSign 创建文件保存各目标消费成功的位置信息，同时备份上一次成功的位置信息，防止进程突然死亡导致位置信息文件为空的情况。

5. 监控任务
启用监控并配置端口，jetty启动成功之后，使用 http://127.0.0.1:端口/moniter 的方式查看任务运行情况。
监控字段说明：

state 状态；rate 开始位置，目标消费位置，保存位置；buffer 队列信息；exception 异常信息；

w 工作任务是否启动；
f oplog抓取任务；
t 目标处理任务；
p 本地位置信息更新任务;
d 守护线程；
c 预警线程；

例子：
{"192.168.246.15|40000|10015":{"buffer":{"position":{"bufferSize":1024,"miniGatingSeq":100421606,"cursor":100421606},"event":{"bufferSize":1024,"miniGatingSeq":100421606,"cursor":100421606}},"rate":{"f":"TS time:Mon Jul 29 14:13:52 CST 2019 inc:51","t":{"10016":"TS time:Thu Aug 01 15:38:39 CST 2019 inc:33316","10017":"TS time:Thu Aug 01 15:38:39 CST 2019 inc:33316","10018":"TS time:Thu Aug 01 15:38:39 CST 2019 inc:33316"},"p":"TS time:Thu Aug 01 15:38:39 CST 2019 inc:33316"},"exception":{"f":"","w":"","d":"","t":{"10016":"","10017":"","10018":""},"c":"","p":""},"state":{"f":true,"w":true,"t":{"10016":true,"10017":true,"10018":true},"p":true}}}

6. 预警线程 CuckooThread
通过获取 strCuckooJSON 预警配置的配置文件信息进行线程初始化

email.json

{
  bDebug: "false",
  bAuth: "true",
  strHost: "smtp.163.com",
  strPort: "25",
  strProtocol: "smtp",
  strSubject: "MO2MO Cuckoo",
  strSendEmail: "",
  strUserName: "",
  strPassword: "",
  strToEmail: ""
}

7. 守护线程 WorkerDaemonThread
每隔3分钟查看一次任务信息，如果任务状态为 false，则对任务进行启动，保证任务能稳定运行