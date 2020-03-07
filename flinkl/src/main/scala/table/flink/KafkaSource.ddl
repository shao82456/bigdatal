CREATE TABLE actor (
  `actor_id` INT,
  first_name VARCHAR,
  last_name VARCHAR
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.topic' = 'test',
  'connector.startup-mode' = 'latest-offset',
  'connector.properties.0.key' = 'zookeeper.connect',
  'connector.properties.0.value' = 'localhost:2181',
  'connector.properties.1.key' = 'bootstrap.servers',
  'connector.properties.1.value' = 'localhost:9092',
  'update-mode' = 'append',
  'format.type' = 'json',
  'format.fail-on-missing-field' = 'true',
  'format.derive-schema' = 'true'
)
