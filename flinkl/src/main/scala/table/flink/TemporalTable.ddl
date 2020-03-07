CREATE TABLE actor_info (
  actor_id INT,
  first_name VARCHAR(45),
  last_name VARCHAR(45),
  film_info VARCHAR
) WITH (
  'connector.type' = 'jdbc',
  'connector.url' = 'jdbc:mysql://localhost:3306/sakila',
  'connector.table' = 'actor_info',
  'connector.driver' = 'com.mysql.jdbc.Driver',
  'connector.username' = 'sakura',
  'connector.password' = 'test'
)