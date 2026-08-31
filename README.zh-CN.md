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
路线和老师原图一致，分为 JSON 直连与文本处理两条通道：

```mermaid
flowchart TD
    PY[Python 日志生成器]
    JSON_LOG[sensor_json.log<br/>半结构化数据]
    TEXT_LOG[sensor_text.log<br/>非结构化数据]
    FB[Fluent Bit]

    JSON_TOPIC[factory-json-topic]
    LS[Logstash]
    GROK[Grok<br/>模式匹配并提取字段]
    MUTATE[Mutate<br/>字符串转换为浮点数]
    TAG[Tagging<br/>添加条件告警元数据]
    TEXT_TOPIC[factory-text-topic]

    KAFKA[Apache Kafka / MSK<br/>消息代理]
    UI[Kafka UI<br/>查看 Topic 和消息]
    VECTOR[Vector]
    FIREHOSE[AWS Firehose<br/>缓冲]
    S3[(S3<br/>仅备份失败文档)]
    OS[(OpenSearch<br/>建立索引与搜索)]
    DASH[OpenSearch Dashboard / Kibana]
    AIRFLOW[Airflow DAG]
    MEDALLION[Medallion 架构]

    PY --> JSON_LOG
    PY --> TEXT_LOG
    JSON_LOG --> FB
    TEXT_LOG --> FB

    FB -->|通道 A：JSON 直连| JSON_TOPIC
    FB -->|通道 B：HTTP 5022| LS
    LS --> GROK --> MUTATE --> TAG --> TEXT_TOPIC

    JSON_TOPIC --> KAFKA
    TEXT_TOPIC --> KAFKA
    KAFKA --> UI
    KAFKA --> VECTOR
    VECTOR --> FIREHOSE
    FIREHOSE --> S3
    FIREHOSE --> OS
    OS <--> DASH
    OS --> AIRFLOW --> MEDALLION
```

> 老师原图说明：Fluent Bit 概念上设计了两条采集路线，实际运行时可按课程阶段选择。Kafka 可以部署在 EC2、Kubernetes 等环境中，也可以使用 AWS MSK。

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
        - 也可以先停止并删除现有容器，再重新启动：
          ```powershell
          docker compose down
          docker compose up -d
          ```
    - 测试
      ```powershell
      # 持续查看 Fluent Bit 日志
      docker logs -f fluent-bit

      # 运行日志生成器
      python log_gen.py
      ```

# Kafka 测试
- 进入 Kafka 容器并创建 Topic
  ```sh
  # 进入容器
  docker container exec -it kafka-local bash

  # 创建名为 spacex 的 Topic
  sh /opt/kafka/bin/kafka-topics.sh --create --topic spacex --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1

  # 启动生产者；输入消息并按 Enter 即可发送
  sh /opt/kafka/bin/kafka-console-producer.sh --topic spacex --bootstrap-server 127.0.0.1:9092

  # 启动消费者，接收 spacex Topic 的消息
  sh /opt/kafka/bin/kafka-console-consumer.sh --topic spacex --bootstrap-server 127.0.0.1:9092
  ```

上述 Topic 配置表示：创建一个名为 `spacex` 的 Topic，使用 1 个分区和 1 个副本，并连接容器内 `127.0.0.1:9092` 上的 Kafka Broker。

- 测试接收 Fluent Bit 发送的传感器消息
  ```sh
  # JSON 数据
  sh /opt/kafka/bin/kafka-console-consumer.sh --topic factory-json-topic --bootstrap-server 127.0.0.1:9092

  # 经 Logstash 处理的文本数据
  sh /opt/kafka/bin/kafka-console-consumer.sh --topic factory-text-topic --bootstrap-server 127.0.0.1:9092
  ```

# Vector
- Vector 是由 Datadog 开发的工具。
- 它使用 Rust 开发，负责收集、转换和传输日志及指标，是一款高性能、轻量级的数据管道工具。
- 查看 Vector 实时日志：
  ```powershell
  docker logs -f vector
  ```
- Vector 输出日志示例：
  ```json
  {
    "@timestamp": 1787890534.104485,
    "headers": {},
    "humidity": 71.7,
    "message_key": null,
    "offset": 18,
    "partition": 0,
    "sensor_id": "AI-FACTORY-001",
    "source_type": "kafka",
    "status": "RUNNING",
    "temperature": 104.1,
    "topic": "factory-json-topic",
    "timestamp": "2026-08-28T04:15:34.604Z",
    "vector_ingest_at": "2026-08-28T04:15:34.617424692Z"
  }
  ```

其中：

- `timestamp`：日志进入 Vector 时由 Kafka source 记录的时间。
- `vector_ingest_at`：Vector 的 remap transform 添加的处理时间。
- 示例中两者相差约 `0.013` 秒，表示消息从 Vector 接收至完成该转换的处理延迟约为 13 毫秒。

# 检查 OpenSearch 中已写入的文档数量

在浏览器中访问索引的 `_count` API：

```text
https://<OpenSearch 域端点>/factory-sensor-001/_count
```

你的当前地址是：

```text
https://search-de-ai-12-kafka-efk-os-cigv4yrkdeqgcbqo6hkcyh2q24.ap-northeast-2.es.amazonaws.com/factory-sensor-001/_count
```

正常响应示例：

```json
{
  "count": 790,
  "_shards": {
    "total": 5,
    "successful": 5,
    "skipped": 0,
    "failed": 0
  }
}
```

- `count`：索引中当前保存的文档数量，实际值会随日志持续写入而变化。
- `successful`：成功参与此次统计的分片数量。
- `failed`：统计失败的分片数量，正常情况下应为 `0`。
