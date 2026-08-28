# 目标
- 理解、使用和应用 Kafka
- 在 ELK、EFK 中：
    - 负责数据传输、过滤、预处理等工作
        - L：Logstash -> 利用相关库，将非结构化数据（文本日志文件）加工后以 JSON 格式传输
        - F：Fluent Bit -> 传输 JSON 或文本数据
        - 传感器设备及其他设备（批量部署）产生日志后，立即通过 Fluent Bit/Logstash 传输到 Kafka，再依次经由 Vector、Firehose 进入 OpenSearch/S3，供搜索服务使用
- OpenSearch
    - Elasticsearch 的 AWS 版本（采用开放版权的版本）
    - 用于搜索引擎和索引查询
    - OpenSearch Dashboard 对应 Kibana

# 目标系统
```
[ 层级 ]           [ 组件 ]               [ 协议 / 操作 ]                  [ 数据格式 ]
====================================================================================================
数据源       :     Python (log_gen.py)  ----( 写入文件 )---->  [ /sensor_logs/*.log ]
               |                                                                   |
               |                                                               (共享卷)
               v                                                                   |
数据采集     :     Fluent Bit（代理）    <---( 持续读取文件 )-----------------------+
               |          |
               |          | [ 通道 A：直接旁路 ]
               |          +--------------------------------------------( 生产 )----+
               |          |                                                         |
               |          | [ 通道 B：代理路由 ]                                    |
               |          +----( 转发 )----> [ Logstash ] ----( 生产 )--------+     |
               |                                     |                        |     |
               v                                     v                        v     v
数据转换     : (A：旁路)   +--[ Grok：模式匹配并提取字段                         ]--+ |
               |           +--[ Mutate：数据类型转换（字符串 -> 浮点数）          ]--+ |
               |           +--[ Tagging：添加用于条件告警的元数据                 ]--+ |
               |                                                              |     |
消息传递     :     Kafka / MSK（代理） <--------------------------------------+-----+
               |          |                （合并结构化数据与处理后的数据）
               |          |
               |          +----( 主题 A：factory-json-topic ) <--- [通道 A 的结果]
               |          +----( 主题 B：factory-text-topic ) <--- [通道 B 的结果]
               v                                                               |
               v                                                               |
                                                                       Kafka Connect / UI
                                                                                |
                                                                                v
可视化       :     OpenSearch（数据库） <---( 建立索引与搜索 )----> Dashboard（Kibana）
====================================================================================================
```

```
                  ┌─ sensor_json.log（半结构化数据）
Python 日志生成器 ┤
                  └─ sensor_text.log（非结构化数据）
                         │
                         ▼
                    Fluent Bit  <- 概念上设计了两个方向（实际只执行其中一个）
                    /         \
                   /           \
             JSON 直连          文本
                │                │
                │               HTTP
                │                ▼
                │             Logstash
                │             Grok 转换：非结构化数据 => 半结构化数据
                │                │
                ▼                ▼
 factory-json-topic      factory-text-topic   <- 主题：用于在 Kafka 中区分消息
             \                 /
              \               /
                 Apache Kafka（代理接收数据）-> 可部署在 AWS 外部、EC2、常驻 Kubernetes，
                                                或使用 MSK（AWS 专用服务）
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
       Kafka UI（仪表板）        Vector（服务）-> 将 Kafka 消息发送到 Firehose
                                  │
                                  ▼
                           AWS Firehose -> 缓冲 -> S3（青铜层）
                                                   或 OpenSearch（按传感器/工厂管理单位建立索引并搜索）
                                                   -> 使用 Airflow DAG
                                                   -> 应用 Medallion 架构
```

# 开发环境配置
- 配置 docker-compose.yaml
```
    docker compose up -d
```

- Kafka 与 Kinesis
    - Kafka 是为处理大规模实时数据而构建的开源分布式事件流平台
    - 核心概念包括生产者、消费者和主题
    - 适合毫秒级、超低延迟的数据传输
    - AWS 以 MSK 服务的形式提供 Kafka
    - 使用较新的方式进行配置

- Fluentd（40～60 MB）与 Fluent Bit（1～5 MB）
    - 基于标签的路由工具
    - Fluentd 采用 Ruby 与 C 结合的结构；Fluent Bit 使用 C
    - 完成服务设置后，主要通过 fluent-bit.conf 进行配置
    - Fluent Bit 的用途
        - 收集、处理和传递数据（ETL 工具）
        - 轻量级开源日志处理器
        - 具有高可用性、高性能处理和稳定的数据处理能力等优点
        - 输入流支持文件、缓冲、路由等大多数常见来源
        - 输出流支持 Kafka、OpenSearch、S3 等大多数服务
            - 可利用相应的插件
    - 修改 fluent-bit.conf 后执行 docker compose restart，使修改生效
