
-- HSTORE OVERRIDE

CREATE OR REPLACE FUNCTION hstore_merge(hstore, hstore)
RETURNS hstore
AS 'SELECT $1 || $2'
LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE AGGREGATE hstore_override(
  sfunc = hstore_merge,
  basetype = hstore,
  stype = hstore,
  initcond = ''
);


-- HSTORE COMBO
CREATE OR REPLACE FUNCTION hstore_merge(hstore, hstore)
RETURNS hstore
AS 'SELECT $1 || $2'
LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;
