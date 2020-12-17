CREATE VIEW citus.time_partitions AS
SELECT partrelid AS "Parent Table", attname AS "Parttiion Column", relid AS "Partition", lower_bound AS "From", upper_bound AS "To", amname AS "Access Method"
FROM (
  SELECT partrelid::regclass AS partrelid, attname, c.oid::regclass AS relid, lower_bound, upper_bound, amname
  FROM pg_class c
  JOIN pg_inherits i ON (c.oid = inhrelid)
  JOIN pg_partitioned_table p ON (inhparent = partrelid)
  JOIN pg_attribute a ON (partrelid = attrelid AND ARRAY[attnum] <@ string_to_array(partattrs::text, ' ')::int2[])
  JOIN pg_type t ON (atttypid = t.oid)
  JOIN pg_namespace tn ON (t.typnamespace = tn.oid)
  LEFT JOIN pg_am am ON (c.relam = am.oid),
  pg_catalog.time_partition_range(c.oid)
  WHERE c.relpartbound IS NOT NULL AND p.partstrat = 'r' AND p.partnatts = 1
) partitions
ORDER BY partrelid::text, lower_bound;

ALTER VIEW citus.time_partitions SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.time_partitions TO public;
