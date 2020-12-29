DROP FUNCTION pg_catalog.undistribute_table(regclass);
CREATE OR REPLACE FUNCTION pg_catalog.undistribute_table(
    table_name regclass, cascade boolean default false)
    RETURNS VOID
    LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$undistribute_table$$;

COMMENT ON FUNCTION pg_catalog.undistribute_table(
    table_name regclass, cascade boolean)
    IS 'undistributes a distributed table';
