CREATE OR REPLACE FUNCTION to_json(anyarray)
  RETURNS character varying AS
'serializer', 'serialize_array'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;


CREATE OR REPLACE FUNCTION to_json(record)
  RETURNS character varying AS
'serializer', 'serialize_record'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;


CREATE OR REPLACE FUNCTION json_agg_transfn( internal, input_record record, array_name text ) 
  RETURNS internal AS
'serializer', 'json_agg_transfn'
  LANGUAGE c IMMUTABLE
  COST 1;

CREATE OR REPLACE FUNCTION json_agg_plain_transfn( internal, input_record record, array_name text )
RETURNS internal AS
'serializer', 'json_agg_plain_transfn'
LANGUAGE c IMMUTABLE
COST 1;

CREATE OR REPLACE FUNCTION json_agg_finalfn(internal)
  RETURNS text AS
'serializer', 'json_agg_finalfn'
  LANGUAGE c IMMUTABLE
  COST 1;

CREATE OR REPLACE FUNCTION json_agg_plain_finalfn(internal)
  RETURNS text AS
'serializer', 'json_agg_plain_finalfn'
  LANGUAGE c IMMUTABLE
  COST 1;


CREATE AGGREGATE json_agg( record, text ) (
  SFUNC=json_agg_transfn,
  STYPE=internal,
  FINALFUNC=json_agg_finalfn
);

CREATE AGGREGATE json_agg_plain( record, text ) (
  SFUNC=json_agg_plain_transfn,
  STYPE=internal,
  FINALFUNC=json_agg_plain_finalfn
);




