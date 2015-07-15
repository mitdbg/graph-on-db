#! /bin/sh


QUERY="SELECT t.from_node, gcd(agg_product(t.to_node),agg_gcd(t.p)) AS gcd
	FROM (
	  SELECT e1.from_node,e1.to_node,e1.to_node*agg_product(e2.to_node) AS p
    	  FROM edge e1, edge e2
    	  WHERE e1.to_node=e2.from_node AND e1.from_node<e1.to_node
    	  GROUP BY e1.from_node,e1.to_node
  	) AS t
  	GROUP BY t.from_node
  	HAVING gcd != t.from_node"


echo -e "\\\timing\n$QUERY" | $VSQL
