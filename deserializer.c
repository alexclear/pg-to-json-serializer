
/*
* @author Maxim Ky
* @date 2011-06-03
* @description pg-to-json-serializer main source file
*/

#include "postgres.h"
#include "fmgr.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "utils/typcache.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/array.h"

#include <string.h>
#include <ctype.h>

#include "common.h"

#ifdef		OPTION_WITH_DESERIALIZER

#define NDEBUG
#include <libjson/libjson.h>


Datum deserialize_record( PG_FUNCTION_ARGS );
Datum deserialize_array( PG_FUNCTION_ARGS );

Datum ConvertFromText( char *value, Oid column_type, MemoryContext fn_mcxt, int4 typmod );

Datum deserialize_record_internal( Oid type_oid, JSONNODE *json_obj, MemoryContext fn_mcxt );
Datum deserialize_array_internal( Oid type_oid, JSONNODE *json_obj, MemoryContext fn_mcxt );

ArrayType* construct_array_with_nulls(	Datum *elems,
	bool *nulls, int nelems, Oid elmtype, int elmlen, bool elmbyval, char elmalign );

// memory allocator callbacks for libjson
static void *allocator(unsigned long bytes);
static void *reallocator(void * buffer, unsigned long bytes);
static void deallocator(void * buffer);

void *allocator(unsigned long bytes)
{
	return palloc( bytes );
}

void *reallocator(void * buffer, unsigned long bytes)
{
	return repalloc( buffer, bytes );
}

void deallocator(void *buffer)
{
	pfree( buffer );
}

Datum ConvertFromText( char *value, Oid column_type, MemoryContext fn_mcxt, int4 typmod )
{
	Oid         typinput;
	Oid         typioparam;
	FmgrInfo    finfo_input;

	getTypeInputInfo(column_type, &typinput, &typioparam);
	fmgr_info_cxt(typinput, &finfo_input, fn_mcxt);

	return InputFunctionCall( &finfo_input, value, typioparam, typmod );
}

ArrayType* construct_array_with_nulls(	Datum *elems,
	bool *nulls, int nelems, Oid elmtype, int elmlen, bool elmbyval, char elmalign )
{
	int dims[1], lbs[1];
	dims[0] = nelems;
	lbs[0] = 1;

	return construct_md_array( elems, nulls, 1, dims, lbs,
		  elmtype, elmlen, elmbyval, elmalign );
}

Datum deserialize_record_internal( Oid type_oid, JSONNODE *json_obj, MemoryContext fn_mcxt )
{
	HeapTuple 			type_tuple;
	Form_pg_attribute *	attrs;
	int					natts;          /* number of attributes */
	int4 				typtypmod;
	TupleDesc			tupdesc;
	int 				i;
	HeapTuple 			tuple;
	Datum *				tuple_values;
	bool *				tuple_isnull;
	int4				mem_size;

	if( json_type( json_obj ) != JSON_NODE )
		elog(ERROR, "non-record type passed to deserialize_record\n" );

	/* obtain type information from pg_catalog */
	type_tuple = SearchSysCache1( TYPEOID, ObjectIdGetDatum( type_oid ) );
	if( ! HeapTupleIsValid( type_tuple ) )
		elog(ERROR, "cache lookup failed for relation %u", type_oid);

	typtypmod = ((Form_pg_type) GETSTRUCT( type_tuple ))->typcategory;
	ReleaseSysCache( type_tuple );

	// get tuple description
	tupdesc = lookup_rowtype_tupdesc(type_oid, typtypmod);
	if( ! tupdesc )
		elog(ERROR, "row type lookup failed for relation %u", type_oid);

	attrs = tupdesc->attrs;
	natts = tupdesc->natts;

	// allocate memory for storing tuple data
	mem_size = sizeof( Datum ) * natts;
	tuple_values = palloc( mem_size );
	memset( tuple_values, 0, mem_size );

	mem_size = sizeof( bool ) * natts;
	tuple_isnull = palloc( mem_size );
	memset( tuple_isnull, 1, mem_size );

	// iterate over all attributes
	for( i = 0; i < natts; ++i )
	{
		const char *		column_name;
		JSONNODE *			column_node;
		Oid		 			column_type = tupdesc->attrs[ i ]->atttypid;
		json_char * 		column_value;
		char 				type_category;
		HeapTuple 			type_tuple;

		/* Ignore dropped columns in datatype */
		if (attrs[ i ]->attisdropped)
			continue;

		/* obtain type information from pg_catalog */
		type_tuple = SearchSysCache1( TYPEOID, ObjectIdGetDatum(column_type) );
		if (!HeapTupleIsValid( type_tuple ))
			elog(ERROR, "cache lookup failed for relation %u (record type)", column_type);

		type_category = ((Form_pg_type) GETSTRUCT( type_tuple ))->typcategory;
		ReleaseSysCache( type_tuple );

		// get column name
		column_name = SPI_fname( tupdesc, i + 1 );

		column_node = json_get( json_obj, column_name );
		// don't fill unexistent columns
		if( ! column_node )
			continue;

		//check for null
		if( json_type( column_node ) == JSON_NULL )
			continue;

		switch( type_category )
		{
			// http://www.postgresql.org/docs/current/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
			case 'A': //array
				tuple_values[ i ] = deserialize_array_internal( column_type, column_node, fn_mcxt );
			break;

			case 'C': //composite
				tuple_values[ i ] = deserialize_record_internal( column_type, column_node, fn_mcxt );
			break;

			default: //another
				// just convert from text representation
				column_value = json_as_string( column_node );

				tuple_values[ i ] = ConvertFromText( column_value,
				column_type, fn_mcxt, attrs[i]->atttypmod );

				json_free( column_value );
		}
		tuple_isnull[ i ] = false;
	}

	ReleaseTupleDesc(tupdesc);

	// create tuple from values and return them
	tuple = heap_form_tuple( tupdesc, tuple_values, tuple_isnull );

	pfree( tuple_values );
	pfree( tuple_isnull );

	return HeapTupleGetDatum( tuple );
}


PG_FUNCTION_INFO_V1( deserialize_record );
Datum deserialize_record( PG_FUNCTION_ARGS )
{
	text *				json_text;
	Oid 				type_oid;
	JSONNODE *			json_obj;
	Datum				result;

	// get argument values
	type_oid = PG_GETARG_OID( 0 );
	json_text = PG_GETARG_TEXT_P( 1 );

	// set postgreSQL allocators as defaults for libjson
	json_register_memory_callbacks( allocator, reallocator, deallocator );

	// parse json
	json_obj = json_parse( PG_TEXT_GET_CSTR( json_text ) );

	if( ! json_obj )
		elog(ERROR, "error parsing json\n" );

	// call internal function for other processing
	result = deserialize_record_internal( type_oid, json_obj, fcinfo->flinfo->fn_mcxt );

	json_delete(json_obj);

	PG_RETURN_DATUM( result );
}

Datum deserialize_array_internal( Oid type_oid, JSONNODE *json_obj, MemoryContext fn_mcxt )
{
	HeapTuple 			type_tuple;
	char 				type_category;
	int4				typmod;
	int2        		typlen;
	bool        		typbyval;
	char        		typalign;
	Oid         		typinput;
	Oid         		typioparam;
	FmgrInfo    		finfo_input;
	Form_pg_type		type_info;
	json_index_t		arr_size;
	json_index_t		i;
	Datum *				arr_elems;
	bool *				arr_nulls;
	int4				mem_size;
	ArrayType *			result;

	if( json_type( json_obj ) != JSON_ARRAY )
		elog(ERROR, "non-array type passed to deserialize_array\n" );

	/* obtain array type information from pg_catalog */
	type_tuple = SearchSysCache1( TYPEOID, ObjectIdGetDatum( type_oid ) );
	if( ! HeapTupleIsValid( type_tuple ) )
		elog(ERROR, "cache lookup failed for relation %u (array type)", type_oid);

	type_oid = ((Form_pg_type)GETSTRUCT( type_tuple ))->typelem;
	ReleaseSysCache( type_tuple );

	/* obtain array element type information from pg_catalog */
	type_tuple = SearchSysCache1( TYPEOID, ObjectIdGetDatum( type_oid ) );
	if( ! HeapTupleIsValid( type_tuple ) )
		elog(ERROR, "cache lookup failed for relation %u (array element type)", type_oid);

	type_info = (Form_pg_type)GETSTRUCT( type_tuple );

	type_category = type_info->typcategory;
	typlen = type_info->typlen;
	typmod = type_info->typtypmod;
	typbyval = type_info->typbyval;
	typalign = type_info->typalign;

	ReleaseSysCache( type_tuple );

	if( type_category != 'C' && type_category != 'A' )
	{
		// get type input info and prepare context for InputFunc
		getTypeInputInfo(type_oid, &typinput, &typioparam);
		fmgr_info_cxt(typinput, &finfo_input, fn_mcxt);
	}

	arr_size = json_size( json_obj );

	// allocate memory for array elements
	mem_size = arr_size * sizeof( Datum );
	arr_elems = palloc( mem_size );
	memset( arr_elems, 0, mem_size );

	// allocate memory for array "is null" flags
	mem_size = arr_size * sizeof( bool );
	arr_nulls = palloc( mem_size );
	memset( arr_nulls, 0, mem_size );

	for( i = 0; i < arr_size; ++ i )
	{
		JSONNODE *			item_node;
		json_char * 		item_value;

		item_node = json_at( json_obj, i );
		if( ! item_node )
		{
			elog(WARNING, "array item %d == nullptr", i); //strange case
			continue;
		}

		//check for null
		if( json_type( item_node ) == JSON_NULL )
		{
			arr_elems[ i ] = 0;
			arr_nulls[ i ] = true;
			continue;
		}

		arr_nulls[ i ] = false;

		switch( type_category )
		{
			// http://www.postgresql.org/docs/current/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
			case 'A': //array - impossible case
			break;

			case 'C': //composite
				arr_elems[ i ] = deserialize_record_internal( type_oid, item_node, fn_mcxt );
			break;

			default: //another
				// just convert from text representation
				item_value = json_as_string( item_node );
				arr_elems[ i ] = InputFunctionCall( &finfo_input, item_value, typioparam, typmod );
				json_free( item_value );
		}
	}

	result = construct_array_with_nulls( arr_elems, arr_nulls, arr_size, type_oid, typlen, typbyval, typalign );

	pfree( arr_elems );
	pfree( arr_nulls );

	return PointerGetDatum( result );
}

PG_FUNCTION_INFO_V1( deserialize_array );
Datum deserialize_array(PG_FUNCTION_ARGS)
{
	text *				json_text;
	Oid 				type_oid;
	JSONNODE *			json_obj;
	Datum				result;

	// get argument values
	type_oid = PG_GETARG_OID( 0 );
	json_text = PG_GETARG_TEXT_P( 1 );

	// set postgreSQL allocators as defaults for libjson
	json_register_memory_callbacks( allocator, reallocator, deallocator );

	// parse json
	json_obj = json_parse( PG_TEXT_GET_CSTR( json_text ) );

	if( ! json_obj )
		elog(ERROR, "error parsing json\n" );

	// call internal function for other processing
	result = deserialize_array_internal( type_oid, json_obj, fcinfo->flinfo->fn_mcxt );

	json_delete(json_obj);

	PG_RETURN_DATUM( result );
}

#endif // OPTION_WITH_DESERIALIZER
