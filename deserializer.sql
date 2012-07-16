
CREATE OR REPLACE FUNCTION arr_from_json( regtype, character varying )
  RETURNS varchar[] AS
'serializer', 'deserialize_array'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;
 
CREATE OR REPLACE FUNCTION from_json(regtype, character varying)
  RETURNS record AS
'serializer', 'deserialize_record'
  LANGUAGE c IMMUTABLE STRICT
  COST 1;

