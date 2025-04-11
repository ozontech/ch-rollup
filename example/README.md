# Example of usage

## Setup
1. Run ClickHouse
```shell
docker run -d --name ch-rollup-clickhouse -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_PASSWORD=ch-rollup clickhouse/clickhouse-server:24.6
```

2. Create table

Connect to ClickHouse:
```shell
docker exec -it ch-rollup-clickhouse clickhouse-client
```

Then create table:
```sql
CREATE TABLE test_table_agg(
  col1 UInt64,
  counter AggregateFunction(count, UInt64),
  rollup_interval UInt64, 
  event_time DateTime
)
  ENGINE=AggregatingMergeTree ORDER BY (col1, counter, event_time) PARTITION BY toYYYYMMDD(event_time);
```

3. Copy ```main.go``` content

Just copy [```main.go```](main.go) content to your code.

4. Run it
```shell
go run main.go
```
