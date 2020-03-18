# KafkaProducer 配置详解
配置|默认值|说明
-|-|-
max.request.size|1048576|最大请求字节数，决定一条记录能否发出，此外broker是否接收还取决于broker的message.max.bytes,replica.fetch.max.bytes的影响，最终能否消费到取决于consumer的max.partition.fetch.bytes
metadata.fetch.timeout.ms|60000|获取topic元信息时的超时时间，如配置的broker不可连接
retries｜0｜重试次数