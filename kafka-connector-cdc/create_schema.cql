CREATE KEYSPACE demo_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1};
CREATE TABLE demo_ks.demo_table (
  k TEXT,
  c TEXT,
  v TEXT,
  PRIMARY KEY ((k), c)
) WITH CLUSTERING ORDER BY (c DESC);

CREATE TABLE demo_ks.demo_table_agg (
  k TEXT,
  elems map<text, text>,
  PRIMARY KEY (k)
);

CREATE TYPE demo_ks.demo_type (
  name TEXT,
  phone TEXT
);

CREATE TABLE demo_ks.demo_table_udt_agg (
  k TEXT,
  elems map<text, frozen<demo_ks.demo_type>>,
  PRIMARY KEY (k)
);

CREATE TYPE demo_ks.entity (
  id text,
  elem_id text,
  source text,
  properties text
);

CREATE TABLE demo_ks.transactions (
  id text PRIMARY KEY,
  entity demo_ks.entity
);

CREATE TABLE demo_ks.resolved (
  id text PRIMARY KEY,
  entities list<demo_ks.entity>
);