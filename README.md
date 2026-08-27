# 목표
- kafka 이해, 활용, 응용
- ELK, EFK에서
    - 데이터 전송/필터링/전처리 등 역활 담당
        - L:Logstash    -> 라이브러리 활용, 비정형데이터(text 로그파일) => 가공 => json 전송
        - F:Fluent-Bit  -> json 전송, text 전송
        - 센서 장비 + 장비등(배치) => 로그 발생 => 바로 전송(Fluent-Bit/Logstash) => kafka 전송 => vector => firhose => opensearch/s3 => 검색 서비스 활용
- opensearch
    - E:엘라스틱서치의 AWS 버전(저작권 오픈된 버전)
    - 검색엔진, 인덱스 검색
    - 대시보드 <-> K:키바나

# 목표 시스템
```
[ LAYER ]          [ COMPONENT ]          [ PROTOCOL / ACTION ]            [ DATA FORMAT ]
====================================================================================================
SOURCE       :     Python (log_gen.py)  ----( Write to File )---->  [ /sensor_logs/*.log ]
               |                                                                   |
               |                                                            ( Shared Volume )
               v                                                                   |
INGESTION    :     Fluent Bit (Agent)   <---( Tailing File  )----------------------+
               |          |
               |          | [ CHANNEL A: DIRECT BYPASS ]
               |          +--------------------------------------------( Produce )--+
               |          |                                                         |
               |          | [ CHANNEL B: PROXY ROUTE  ]                             |
               |          +----( Forward )----> [ Logstash ] ----( Produce )--+     |
               |                                     |                        |     |
               v                                     v                        v     v
TRANSFORM    : (A: Bypass)  +--[ Grok    : Pattern Matching & Extract Fields   ]--+ |
               |            +--[ Mutate  : Data Type Conversion (Str->Float)   ]--+ |
               |            +--[ Tagging : Add Metadata for Conditional Alerts ]--+ |
               |                                                              |     |
MESSAGING    :     Kafka / MSK (Broker) <-------------------------------------+-----+
               |          |             ( Merge Structured & Processed Data )
               |          |
               |          +----( Topic A: factory-json-topic ) <--- [Channel A Result]
               |          +----( Topic B: factory-text-topic ) <--- [Channel B Result]
               v                                                               |
               v                                                               |
                                                                        kafka connect / ui
                                                                                |
                                                                                v               
VISUALIZE    :     OpenSearch (DB)      <---( Indexing & Search )---->  DASHBOARD (Kibana)
====================================================================================================
```

```
                  ┌─ sensor_json.log (반정형 데이터)
Python 로그 생성기 ┤
                  └─ sensor_text.log (비정형 데이터)
                         │
                         ▼
                    Fluent Bit
                    /         \
                   /           \
          JSON 직결             TEXT
             │                  │
             │               HTTP
             │                  ▼
             │              Logstash
             │              Grok 변환 -> 비정형 => 반정형 처리
             │                  │
             ▼                  ▼
 factory-json-topic      factory-text-topic
             \                 /
              \               /
                 Apache Kafka (브로커 데이터 수신) -> AWS 외부 or EC2 or 쿠버네티스 상주 or MSK(전용 AWS 서비스)
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
       Kafka UI(대시보드)        Vector (서비스)
                                  │
                                  ▼
                           AWS Firehose -> 버퍼링 -> s3(브론즈) or opensearch(인덱스(센서/공장관리단위) 검색)
```