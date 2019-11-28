INSERT INTO demo_ks.demo_table (k, c, v) VALUES ('a', '1', 'b');
INSERT INTO demo_ks.demo_table (k, c, v) VALUES ('a', '2', 'b');
INSERT INTO demo_ks.demo_table (k, c, v) VALUES ('a', '3', 'b');
INSERT INTO demo_ks.demo_table (k, c, v) VALUES ('c', '1', 'd');
INSERT INTO demo_ks.demo_table (k, c, v) VALUES ('c', '2', 'd');
INSERT INTO demo_ks.demo_table (k, c, v) VALUES ('c', '3', 'd');
INSERT INTO demo_ks.demo_table (k, c, v) VALUES ('e', '1', 'f');
INSERT INTO demo_ks.demo_table (k, c, v) VALUES ('e', '2', 'f');

INSERT INTO demo_ks.demo_table_udt (k, c, elem) VALUES ('a', '1', {name: 'n1', phone: 'p1'});
INSERT INTO demo_ks.demo_table_udt (k, c, elem) VALUES ('b', '1', {name: 'n1', phone: 'p1'});
INSERT INTO demo_ks.demo_table_udt (k, c, elem) VALUES ('c', '1', {name: 'n1', phone: 'p1'});
INSERT INTO demo_ks.demo_table_udt (k, c, elem) VALUES ('a', '2', {name: 'n2', phone: 'p2'});
INSERT INTO demo_ks.demo_table_udt (k, c, elem) VALUES ('b', '2', {name: 'n2', phone: 'p2'});
INSERT INTO demo_ks.demo_table_udt (k, c, elem) VALUES ('c', '2', {name: 'n2', phone: 'p2'});

INSERT INTO demo_ks.demo_table_udt_agg (k, elems) VALUES ('a', {'1': {name: 'n1', phone: 'p1'}});
update demo_ks.demo_table_udt_agg set elems = elems + {'2': {name: 'n2', phone: 'p2'}} where k = 'a';
update demo_ks.demo_table_udt_agg set elems = elems + {'1': {name: 'n2'}} where k = 'a';
