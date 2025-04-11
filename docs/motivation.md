# Motivation

The initial problem was that we wanted to store data for several months, but we could only store data for a few weeks (in some tables, only a week) because they took up too much disk space. To solve this problem, we decided to roll up the data and began comparing different implementation options for data roll up. We have summarized the comparison below.

| Feature                                      | ch-rollup | [TTL Group By](https://clickhouse.com/docs/en/guides/developer/ttl#implementing-a-rollup) | Multiple tables                                         | 
|----------------------------------------------|-----------|-------------------------------------------------------------------------------------------|---------------------------------------------------------|
| Doesn't makes scheme edit more difficult     | ✅         | ❌ (adding new column to group by statement trigger PK change => table recreation)         | ❌ (need to apply migrations to all tables)              |
| Doesn't complicate ```SELECT``` queries      | ✅         | ✅                                                                                         | ❌  (need to do ```SELECT``` from several tables at once |
| Easy to change intervals                     | ✅         | ❌ (new intervals trigger PK change => table recreation)                                   | ✅                                                       |
| No impact on ClickHouse performance          | ✅         | ❌ (additional columns in ```Order By```)                                                  | ❌                                                       |
| The logic is implemented inside the database | ❌         | ✅                                                                                         | ✅                                                       |
| Easy to monitor                              | ✅         | ❌ (it's not clear when the roll up doesn't work)                                          | ✅                                                       |
| Used in production                           | ✅         | ❌ (have bugs that doesn't fix in more than a year)                                        | ✅                                                       |
