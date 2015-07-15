-- set data files from environment variables
\set dir `pwd`/ 
\set nodeFile '''':dir`echo $NODES`''''
\set edgeFile '''':dir`echo $EDGES`''''

-- load nodes
DROP TABLE IF EXISTS node CASCADE;
CREATE TABLE node (id int NOT NULL PRIMARY KEY, value int NOT NULL) ORDER BY Id SEGMENTED BY Hash(id) ALL NODES;
COPY node FROM :nodeFile DELIMITER ' ' NULL 'null';

-- load edges
DROP TABLE IF EXISTS edge CASCADE;
CREATE TABLE edge (from_node INT NOT NULL, to_node INT NOT NULL) ORDER BY from_node SEGMENTED BY Hash(from_node) ALL NODES;
COPY edge FROM :edgeFile DELIMITER ' ' NULL 'null';

-- create projections
DROP PROJECTION IF EXISTS edge_p;
CREATE PROJECTION edge_p AS SELECT from_node,to_node from edge ORDER BY to_node UNSEGMENTED ALL NODES;
SELECT start_refresh();

-- drop user defined functions
DROP AGGREGATE FUNCTION IF EXISTS agg_product(int);
DROP AGGREGATE FUNCTION IF EXISTS agg_gcd(int);
DROP FUNCTION IF EXISTS gcd(int,int);

-- recreate the library
DROP LIBRARY IF EXISTS MyFunctions;
\set functionFile '''':dir'/MyFunctions.so'''
CREATE LIBRARY MyFunctions AS :functionFile;

-- create the user defined functions
CREATE AGGREGATE FUNCTION agg_product as language 'C++' name 'ProductFactory' library MyFunctions;
CREATE AGGREGATE FUNCTION agg_gcd as language 'C++' name 'GCDFactory' library MyFunctions;
CREATE FUNCTION gcd as language 'C++' name 'GCDFactory_scalar' library MyFunctions NOT FENCED;

-- collect statistics, drop caches
SELECT ANALYZE_STATISTICS ('');
SELECT clear_caches();
