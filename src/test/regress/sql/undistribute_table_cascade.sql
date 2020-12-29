\set VERBOSITY terse

SET citus.next_shard_id TO 1515000;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA undistribute_table_cascade;
SET search_path TO undistribute_table_cascade;

SET client_min_messages to ERROR;

-- ensure that coordinator is added to pg_dist_node
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

CREATE TABLE reference_table_1 (col_1 INT UNIQUE, col_2 INT UNIQUE, UNIQUE (col_2, col_1));
CREATE TABLE reference_table_2 (col_1 INT UNIQUE, col_2 INT UNIQUE);
SELECT create_reference_table('reference_table_1');
SELECT create_reference_table('reference_table_2');

CREATE TABLE distributed_table_1 (col_1 INT UNIQUE);
CREATE TABLE distributed_table_2 (col_1 INT UNIQUE);
CREATE TABLE distributed_table_3 (col_1 INT UNIQUE);
SELECT create_distributed_table('distributed_table_1', 'col_1');
SELECT create_distributed_table('distributed_table_2', 'col_1');
SELECT create_distributed_table('distributed_table_3', 'col_1');

CREATE TABLE citus_local_table_1 (col_1 INT UNIQUE);
CREATE TABLE citus_local_table_2 (col_1 INT UNIQUE);
SELECT create_citus_local_table('citus_local_table_1');
SELECT create_citus_local_table('citus_local_table_2');

ALTER TABLE distributed_table_3 ADD CONSTRAINT fkey_1 FOREIGN KEY (col_1) REFERENCES distributed_table_2(col_1);
ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_2 FOREIGN KEY (col_1) REFERENCES distributed_table_3(col_1);
ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_3 FOREIGN KEY (col_1) REFERENCES distributed_table_1(col_1);
ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_4 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_1);
ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_5 FOREIGN KEY (col_1, col_2) REFERENCES reference_table_1(col_2, col_1);
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_6 FOREIGN KEY (col_1) REFERENCES reference_table_1(col_2);
ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_7 FOREIGN KEY (col_1) REFERENCES citus_local_table_2(col_1);

-- show that all of below fails as we didn't provide cascade=true
SELECT undistribute_table('distributed_table_1');
SELECT undistribute_table('citus_local_table_1');
SELECT undistribute_table('reference_table_2');

-- In each of below transation blocks, show that we preserve foreign keys.
-- Also show that we don't have any citus tables in current schema after
-- undistribute_table(cascade).
-- So in each transaction, both selects should return true.

BEGIN;
  SELECT undistribute_table('distributed_table_2', cascade=>true);

  -- show that we switch to sequential execution
  show citus.multi_shard_modify_mode;

  SELECT COUNT(*)=7 FROM pg_constraint WHERE conname ~ '^fkey\_\d+$';

  SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='undistribute_table_cascade';
ROLLBACK;

BEGIN;
  SELECT undistribute_table('reference_table_1', cascade=>true);

  SELECT COUNT(*)=7 FROM pg_constraint WHERE conname ~ '^fkey\_\d+$';

  SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='undistribute_table_cascade';
ROLLBACK;

BEGIN;
  SELECT undistribute_table('citus_local_table_1', cascade=>true);

  -- print foreign keys only in one of xact blocks not to make tests too verbose
  SELECT conname, conrelid::regclass, confrelid::regclass
  FROM pg_constraint WHERE conname ~ '^fkey\_\d+$' ORDER BY conname;

  SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
  WHERE tablename=logicalrelid::regclass::text AND
        schemaname='undistribute_table_cascade';
ROLLBACK;

BEGIN;
  SELECT COUNT(*) FROM distributed_table_1;
  -- show that we error out as select gets executed in parallel mode
  SELECT undistribute_table('reference_table_1', cascade=>true);
ROLLBACK;

-- split distributed_table_2 & distributed_table_3 into a seperate foreign
-- key graph then undistribute them
ALTER TABLE distributed_table_2 DROP CONSTRAINT fkey_3;
SELECT undistribute_table('distributed_table_2', cascade=>true);

-- should return true as we undistributed those two tables
SELECT COUNT(*)=0 FROM pg_dist_partition, pg_tables
WHERE tablename=logicalrelid::regclass::text AND
      schemaname='undistribute_table_cascade' AND
      (tablename='distributed_table_2' OR tablename='distributed_table_3');

-- other tables should stay as is since we splited those two tables
SELECT COUNT(*)=5 FROM pg_dist_partition, pg_tables
WHERE tablename=logicalrelid::regclass::text AND
      schemaname='undistribute_table_cascade';

-- test partitioned tables
CREATE TABLE partitioned_table_1 (col_1 INT UNIQUE, col_2 INT) PARTITION BY RANGE (col_1);
CREATE TABLE partitioned_table_1_100_200 PARTITION OF partitioned_table_1 FOR VALUES FROM (100) TO (200);
CREATE TABLE partitioned_table_1_200_300 PARTITION OF partitioned_table_1 FOR VALUES FROM (200) TO (300);
SELECT create_distributed_table('partitioned_table_1', 'col_1');

CREATE TABLE partitioned_table_2 (col_1 INT UNIQUE, col_2 INT) PARTITION BY RANGE (col_1);
CREATE TABLE partitioned_table_2_100_200 PARTITION OF partitioned_table_2 FOR VALUES FROM (100) TO (200);
CREATE TABLE partitioned_table_2_200_300 PARTITION OF partitioned_table_2 FOR VALUES FROM (200) TO (300);
SELECT create_distributed_table('partitioned_table_2', 'col_1');

CREATE TABLE reference_table_3 (col_1 INT UNIQUE, col_2 INT UNIQUE);
SELECT create_reference_table('reference_table_3');

ALTER TABLE partitioned_table_1 ADD CONSTRAINT fkey_8 FOREIGN KEY (col_1) REFERENCES reference_table_3(col_2);
ALTER TABLE partitioned_table_2 ADD CONSTRAINT fkey_9 FOREIGN KEY (col_1) REFERENCES reference_table_3(col_2);

BEGIN;
  SELECT undistribute_table('partitioned_table_2', cascade=>true);

  -- show that we preserve foreign keys on partitions too
  SELECT conname, conrelid::regclass, confrelid::regclass
  FROM pg_constraint
  WHERE conname = 'fkey_8' OR conname = 'fkey_9'
  ORDER BY 1,2,3;
ROLLBACK;

-- cleanup at exit
DROP SCHEMA undistribute_table_cascade CASCADE;
