-- citus--9.5-1--10.0-1

DROP FUNCTION IF EXISTS pg_catalog.citus_total_relation_size(regclass);

#include "udfs/citus_total_relation_size/10.0-1.sql"
#include "udfs/citus_tables/10.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/10.0-1.sql"

#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"

-- Remove all the appended shardId from the names of constraints of partitioned tables,
-- with the exception of foreign key constraints that should keep the shardId in their names
DO $$DECLARE r record;
BEGIN
    FOR r IN SELECT cls.relname::text table_name,
					cons.conname prev_constraint_name,
                    constraint_name_and_shardid[1] as new_constraint_name
                FROM pg_catalog.pg_constraint cons
                    JOIN pg_class cls ON cons.conrelid = cls.oid,
                    -- capture the original constraint name and the shardid in separate groups
                    regexp_match(cons.conname, '^(.*)_([0-9]+)$') as constraint_name_and_shardid,
                    -- capture the parent table name and the shardid in separate groups
                    regexp_match(cls.relname::text, '^(.*)_([0-9]+)$') as parent_name_and_shardid
                WHERE
                    -- ignore foreign key constraints
                    cons.contype <> 'f'
                    -- find only partitioned tables
                    AND cls.relkind = 'p'
                    -- check that the shardid in the constraint name and the parent table are the same
                    AND parent_name_and_shardid[2] = constraint_name_and_shardid[2]
                ORDER BY 1,2,3
    LOOP
        EXECUTE 'ALTER TABLE ' || quote_ident(r.table_name) || ' RENAME CONSTRAINT ' || quote_ident(r.prev_constraint_name) || ' TO '  || quote_ident(r.new_constraint_name);
    END LOOP;
END$$;
