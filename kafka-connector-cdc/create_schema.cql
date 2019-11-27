CREATE KEYSPACE demo_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1};
CREATE TABLE demo_ks.demo_table (
  k TEXT,
  c TEXT,
  v TEXT,
  PRIMARY KEY ((k), c)
) WITH CLUSTERING ORDER BY (c DESC);