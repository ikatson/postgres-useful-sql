BEGIN;

DROP SCHEMA IF EXISTS adm CASCADE;
CREATE SCHEMA adm;

CREATE EXTENSION IF NOT EXISTS pg_buffercache;

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

COMMENT ON VIEW adm.table_scans_with_sizes IS 'View table sizes + seq scan and idx scan info. Useful to analyze how often seqscans are executed on large tables';


-- Total on-disk sizes of all schemas of current database;
CREATE VIEW adm.schema_sizes AS
    select  schemaname
            ,sum(pg_total_relation_size(schemaname||'.'||tablename))::bigint as size_bytes
            ,pg_size_pretty(sum(pg_total_relation_size(schemaname||'.'||tablename))::bigint) as size
            from pg_tables
            where schemaname != 'information_schema'
            group by schemaname
            order by size_bytes desc;

COMMENT ON VIEW adm.schema_sizes IS 'Total on-disk sizes of all schemas of current database';

-- Show the size of the provided DB
CREATE FUNCTION adm.db_size (db text) RETURNS text AS $$
   SELECT pg_size_pretty(pg_database_size($1));
$$ language sql;

-- The sizes of all DBs
CREATE VIEW adm.size_all AS
   SELECT datname AS database, pg_database_size(datname) AS size,
   pg_size_pretty(pg_database_size(datname)) AS pretty_size
   FROM pg_database;

-- This one shows the idex usage
CREATE VIEW adm.index_use AS
   SELECT
     indexrelname,
     idx_tup_read,
     idx_tup_fetch,
     (idx_tup_read - idx_tup_fetch),
     CASE WHEN idx_tup_read = 0 THEN 0 ELSE (idx_tup_read::float4 -
   idx_tup_fetch) / idx_tup_read END as r
   FROM
     pg_stat_user_indexes
   ORDER BY r desc;

-- This one is used to parse the explain results to replace SELECT count(*)
CREATE OR REPLACE FUNCTION adm.count_estimate(query text) returns integer as $$
declare
	rec record;
	rows integer;
begin
	for rec in execute 'EXPLAIN ' || query loop
		rows := substring(rec."QUERY PLAN" from ' rows=([[:digit:]]+)');
		exit when rows is not null;
	end loop;
	return rows;
end;
$$ language plpgsql strict;

-- This view displays tables without primary keys. Useful for londiste replication.
CREATE OR REPLACE VIEW adm.tables_without_pk AS SELECT
    n.nspname AS "Schema",
    c.relname AS "Table Name",
    c.relhaspkey AS "Has PK"
    FROM
        pg_catalog.pg_class c
    JOIN
        pg_namespace n
    ON (c.relnamespace = n.oid
        AND n.nspname NOT IN ('information_schema', 'pg_catalog')
        AND c.relkind='r' ) where c.relhaspkey = 'f';

-- This view displays approximate table bloat. Taken from check_postgres.pl. Needs to be updated.
CREATE OR REPLACE VIEW adm.bloat AS
SELECT
  schemaname, tablename, reltuples::bigint, relpages::bigint, otta,
    ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
      CASE WHEN relpages < otta THEN 0 ELSE relpages::bigint - otta END AS wastedpages,
        CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wastedbytes,
	  CASE WHEN relpages < otta THEN pg_size_pretty(0::bigint) ELSE pg_size_pretty((bs*(relpages-otta))::bigint) END AS wastedsize,
	    iname, ituples::bigint, ipages::bigint, iotta,
	      ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
	        CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
		  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
		    CASE WHEN ipages < iotta THEN pg_size_pretty(0::bigint) ELSE pg_size_pretty((bs*(ipages-iotta))::bigint) END AS wastedisize
		    FROM (
		      SELECT
		          schemaname, tablename, cc.reltuples, cc.relpages, bs,
			      CEIL((cc.reltuples*((datahdr+ma-
			            (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
				        COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
					    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
					      FROM (
					          SELECT
						        ma,bs,schemaname,tablename,
							      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
							            (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
								        FROM (
									      SELECT
									              schemaname, tablename, hdr, ma, bs,
										              SUM((1-null_frac)*avg_width) AS datawidth,
											              MAX(null_frac) AS maxfracsum,
												              hdr+(
													                SELECT 1+count(*)/8
															          FROM pg_stats s2
																            WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
																	            ) AS nullhdr
																		          FROM pg_stats s, (
																			          SELECT
																				            (SELECT current_setting('block_size')::numeric) AS bs,
																					              CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
																						                CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
																								        FROM (SELECT version() AS v) AS foo
																									      ) AS constants
																									            GROUP BY 1,2,3,4,5
																										        ) AS foo
																											  ) AS rs
																											    JOIN pg_class cc ON cc.relname = rs.tablename
																											      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
																											        LEFT JOIN pg_index i ON indrelid = cc.oid
																												  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
																												  ) AS sml
																												  WHERE sml.relpages - otta > 10 OR ipages - iotta > 10
																												  ORDER BY wastedbytes DESC;


CREATE OR REPLACE FUNCTION adm.grant_on_tables(role_name text, permission text, mask text, schema_name text) RETURNS integer
    AS $$
-- Function that grants given permissions to given role on tables with given LIKE mask within given schema
-- Example:
--   SELECT grant_on_tables('role_developer','SELECT, INSERT, UPDATE, DELETE, RULE, REFERENCE, TRIGGER','%','public');
-- will grant all the maximum permissions on all tables within public schema to role_developer role
DECLARE
	obj record;
	num integer;
BEGIN
	num := 0;
	FOR obj IN
			SELECT relname FROM  pg_class c JOIN pg_namespace ns ON (c.relnamespace = ns.oid)
			WHERE relkind in ('r','v','S')  AND nspname = schema_name  AND relname LIKE mask
			ORDER BY relname
	LOOP
		EXECUTE 'GRANT ' || permission || ' ON ' || obj.relname || ' TO ' || role_name;
		RAISE NOTICE '%', 'Done: GRANT ' || permission || ' ON ' || obj.relname || ' TO ' || role_name;
		num := num + 1;
	END LOOP;
	RETURN num;
END;
$$ language plpgsql;

-- How many shared buffers are in the database.
-- Taken from the awesome "PostgreSQL 9.0 High Performance" book by Greg Smith.
CREATE VIEW adm.buffers_count AS
SELECT
  setting AS shared_buffers,
  pg_size_pretty((SELECT setting FROM pg_settings WHERE name='block_size')::int8 * setting::int8) AS size
FROM pg_settings WHERE name='shared_buffers';

-- How many buffers does each table use. Taken from pg_buffercache documentation
CREATE VIEW adm.buffers_use AS
SELECT
  c.relname,
  count(*) AS buffers
FROM pg_class c
  INNER JOIN pg_buffercache b
    ON b.relfilenode=c.relfilenode
  INNER JOIN pg_database d
    ON (b.reldatabase=d.oid AND d.datname=current_database())
GROUP BY c.relname
ORDER BY 2 DESC;

-- Taken from the awesome "PostgreSQL 9.0 High Performance" book by Greg Smith.
-- Buffer contents summary, with percentages
CREATE VIEW adm.buffers_breakdown AS
SELECT
  c.relname,
  pg_size_pretty(count(*) * 8192) as buffered,
  round(100.0 * count(*) /
    (SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1)
    AS buffers_percent,
  round(100.0 * count(*) * 8192 / pg_relation_size(c.oid),1)
    AS percent_of_relation
FROM pg_class c
  INNER JOIN pg_buffercache b
    ON b.relfilenode = c.relfilenode
  INNER JOIN pg_database d
    ON (b.reldatabase = d.oid AND d.datname = current_database())
GROUP BY c.oid,c.relname
ORDER BY 3 DESC
LIMIT 10;

COMMIT;