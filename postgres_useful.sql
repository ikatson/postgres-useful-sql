BEGIN;

DROP SCHEMA IF EXISTS adm CASCADE;
CREATE SCHEMA adm;

-- View table and index sizes
CREATE VIEW adm.table_sizes AS
SELECT n.nspname, c.relname, c.relkind AS type,
    pg_size_pretty(pg_table_size(c.oid::regclass)) AS size, 
    pg_size_pretty(pg_indexes_size(c.oid::regclass)) AS idxsize, 
    pg_size_pretty(pg_total_relation_size(c.oid::regclass)) AS total,

    pg_table_size(c.oid::regclass) AS size_raw,
    pg_indexes_size(c.oid::regclass) AS idxsize_raw,
    pg_total_relation_size(c.oid::regclass) AS total_raw,
    c.oid as rel_oid,
    n.oid as schema_oid,
    c.relkind as relkind
   FROM pg_class c
   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE (n.nspname <> ALL (ARRAY['pg_catalog'::name, 'information_schema'::name])) AND n.nspname !~ '^pg_toast'::text AND (c.relkind = ANY (ARRAY['r'::"char", 'i'::"char"]))
  ORDER BY pg_total_relation_size(c.oid::regclass) DESC;


-- Same as above, but with seq scan and idx scan info. Useful to extract seqscan info on large tables.
CREATE VIEW adm.table_scans_with_sizes AS
SELECT
  tsize.*,
  tstat.seq_scan, tstat.seq_tup_read, tstat.idx_scan, tstat.idx_tup_fetch
FROM
  adm.table_sizes tsize,
  pg_stat_all_tables tstat
WHERE
tsize.rel_oid = tstat.relid
ORDER BY
tstat.seq_scan * tsize.size_raw DESC;