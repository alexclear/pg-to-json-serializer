
/*
* @author Maxim Ky
* @date 2011-06-03
* @description pg-to-json-serializer serialize utils source file
*/

#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include <string.h>
#include "executor/executor.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"
#include "utils/array.h"

#include "common.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


Datum serialize_record( PG_FUNCTION_ARGS );
Datum serialize_array( PG_FUNCTION_ARGS );
char *ConvertToText( Datum, Oid, MemoryContext );
void appendStringInfoQuotedString( StringInfo, const char * );

Datum json_agg_finalfn( PG_FUNCTION_ARGS );
Datum json_agg_transfn( PG_FUNCTION_ARGS );
Datum json_agg_plain_finalfn( PG_FUNCTION_ARGS );
Datum json_agg_plain_transfn( PG_FUNCTION_ARGS );
static Datum json_agg_common_transfn( PG_FUNCTION_ARGS, bool top_object );
static Datum json_agg_common_finalfn( PG_FUNCTION_ARGS, bool top_object );

//----------------------------------------------------------


void appendStringInfoQuotedString( StringInfo buf, const char *string )
{
	appendStringInfoChar( buf, '"'); //enclose input strings with quotes

	for( ; *string; ++ string )
	{
		switch( *string )
		{
			case '"': case '\\':
				appendStringInfoChar( buf, '\\');
			break;
		}

		appendStringInfoChar( buf, *string);
	}

	appendStringInfoChar( buf, '"');
}

char *ConvertToText( Datum value, Oid column_type, MemoryContext fn_mcxt )
{
	bool typIsVarlena;
	Oid typiofunc;
	FmgrInfo	proc;

	getTypeOutputInfo(column_type, &typiofunc, &typIsVarlena);
	fmgr_info_cxt( typiofunc, &proc, fn_mcxt );

	return OutputFunctionCall( &proc, value );
}

PG_FUNCTION_INFO_V1( serialize_record );
Datum serialize_record( PG_FUNCTION_ARGS )
{
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);

	HeapTupleData tuple;
	bool		needComma = false;
	int		 i;
	Datum	  *values;
	bool	   *nulls;
	StringInfoData buf;

	/* Extract type info from the tuple itself */
	Oid tupType = HeapTupleHeaderGetTypeId(rec);
	int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
	TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	int ncolumns = tupdesc->natts;

	/* Build a temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;


	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	/* Break down the tuple into fields */
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	/* And build the result string */
	initStringInfo(&buf);

	appendStringInfoChar(&buf, '{');

	for (i = 0; i < ncolumns; i++)
	{
		Oid		 column_type = tupdesc->attrs[ i ]->atttypid;
		char	   *value;
		char	   *column_name;
		char 		type_category;
		HeapTuple 	type_tuple;
		FmgrInfo flinfo;

		/* Ignore dropped columns in datatype */
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		if (nulls[i])
		{
			/* emit nothing... */
			continue;
		}

		if (needComma)
			appendStringInfoChar(&buf, ',');

		needComma = true;


		/* obtain column name */
		column_name = SPI_fname( tupdesc, i + 1 );

		/* obtain type information from pg_catalog */
		type_tuple = SearchSysCache1( TYPEOID, ObjectIdGetDatum(column_type) );
		if (!HeapTupleIsValid( type_tuple ))
			elog(ERROR, "cache lookup failed for relation %u", column_type);

		type_category = ((Form_pg_type) GETSTRUCT( type_tuple ))->typcategory;

		ReleaseSysCache( type_tuple );

		/* append column name */
		appendStringInfoChar(&buf, '"');
		appendStringInfoString(&buf, column_name);
		appendStringInfoString(&buf, "\":");

		switch( type_category )
		{
			// http://www.postgresql.org/docs/current/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE

			case 'A': //array
				//call to serialize_array( ... )

				MemSet( &flinfo, 0, sizeof( flinfo ) );
				flinfo.fn_addr = serialize_array;
				flinfo.fn_nargs = 1;
				flinfo.fn_mcxt = fcinfo->flinfo->fn_mcxt;

				value = PG_TEXT_DATUM_GET_CSTR( FunctionCall1( &flinfo, values[ i ] ) );

				appendStringInfoString(&buf, value);
			break;

			case 'C': //composite
				//recursive call to serialize_record( ... )
				MemSet( &flinfo, 0, sizeof( flinfo ) );
				flinfo.fn_addr = serialize_record;
				flinfo.fn_nargs = 1;
				flinfo.fn_mcxt = fcinfo->flinfo->fn_mcxt;

				value = PG_TEXT_DATUM_GET_CSTR( FunctionCall1( &flinfo, values[ i ] ) );

				appendStringInfoString(&buf, value);
			break;

			case 'N': //numeric

				// get column text value
				value = ConvertToText( values[ i ], column_type, fcinfo->flinfo->fn_mcxt );

				appendStringInfoString(&buf, value);

			break;

			case 'B': //boolean
				appendStringInfoString(&buf,
					// get column boolean value
					DatumGetBool( values[ i ] ) ? "true" : "false"
				);
			break;

			default: //another

				// get column text value
				value = ConvertToText( values[ i ], column_type, fcinfo->flinfo->fn_mcxt );

				appendStringInfoQuotedString(&buf, value);
		}
	}

	appendStringInfoChar(&buf, '}');

	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	PG_RETURN_TEXT_P( PG_CSTR_GET_TEXT( buf.data ) );
}

PG_FUNCTION_INFO_V1( serialize_array );
Datum serialize_array(PG_FUNCTION_ARGS)
{
	ArrayType  *v = PG_GETARG_ARRAYTYPE_P(0);
	Oid		 element_type = ARR_ELEMTYPE(v);
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;
	char	   *p, *value;
	char 		type_category;
	bool		needComma = false;

	bits8	  *bitmap;
	int		 bitmask;
	int		 nitems, i;
	int		 ndim, *dims;

	Oid			typioparam, typiofunc;
	HeapTuple 	type_tuple;
	FmgrInfo	proc, flinfo;

	StringInfoData buf;

	/*
	 * Get info about element type, including its output conversion proc
	 */
	get_type_io_data(element_type, IOFunc_output,
					 &typlen, &typbyval,
					 &typalign, &typdelim,
					 &typioparam, &typiofunc);

	fmgr_info_cxt( typiofunc, &proc, fcinfo->flinfo->fn_mcxt );

	ndim = ARR_NDIM(v);
	dims = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndim, dims);

	if( ndim > 1 )
		elog( ERROR, "multidimensional arrays doesn't supported" );

	p = ARR_DATA_PTR(v);
	bitmap = ARR_NULLBITMAP(v);
	bitmask = 1;

	/* obtain type information from pg_catalog */
	type_tuple = SearchSysCache1( TYPEOID, ObjectIdGetDatum(element_type) );
	if (!HeapTupleIsValid( type_tuple ))
		elog(ERROR, "cache lookup failed for relation %u", element_type);

	type_category = ((Form_pg_type) GETSTRUCT( type_tuple ))->typcategory;

	ReleaseSysCache( type_tuple );

	/* Build the result string */
	initStringInfo(&buf);

	appendStringInfoChar(&buf, '[');
	for (i = 0; i < nitems; i++)
	{
		if (needComma)
			appendStringInfoChar(&buf, ',');
		needComma = true;

		/* Get source element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			// append null
			appendStringInfoString(&buf, "null");
		}
		else
		{
			/* get item value and advance array data pointer */
			Datum itemvalue = fetch_att(p, typbyval, typlen);
			p = att_addlength_pointer(p, typlen, p);
			p = (char *) att_align_nominal(p, typalign);

			//------------------------
			switch( type_category )
			{
				// http://www.postgresql.org/docs/current/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE

				case 'A': //array - impossible case
				break;

				case 'C': //composite
					//call to serialize_record( ... )
					MemSet( &flinfo, 0, sizeof( flinfo ) );
					flinfo.fn_addr = serialize_record;
					flinfo.fn_nargs = 1;
					flinfo.fn_mcxt = fcinfo->flinfo->fn_mcxt;

					value = PG_TEXT_DATUM_GET_CSTR( FunctionCall1( &flinfo, itemvalue ) );

					appendStringInfoString(&buf, value);
				break;

				case 'N': //numeric

					// get column text value
					value = OutputFunctionCall( &proc, itemvalue );

					appendStringInfoString(&buf, value);
				break;

				default: //another

					// get column text value
					value = OutputFunctionCall( &proc, itemvalue );

					appendStringInfoQuotedString(&buf, value);
			}
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}
	appendStringInfoChar(&buf, ']');

	//PG_RETURN_CSTRING(retval);
	PG_RETURN_TEXT_P( PG_CSTR_GET_TEXT( buf.data ) );
}

//========================================================================================================================
//=
//= aggregate staff
//=
//========================================================================================================================

static StringInfo makeJsonAggState( FunctionCallInfo fcinfo )
{
	StringInfo  state;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "json_agg_*_transfn called in non-aggregate context");
	}

	/*
	 * Create state in aggregate context.  It'll stay there across subsequent
	 * calls.
	 */
	oldcontext = MemoryContextSwitchTo(aggcontext);
	state = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);

	return state;
}

Datum json_agg_common_transfn( PG_FUNCTION_ARGS, bool top_object )
{
	StringInfo state;
	char *serialized_value;
	FmgrInfo	flinfo;

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	/* Append the value unless null. */
	if (!PG_ARGISNULL(1))
	{
		/* On the first time through, we ignore the delimiter. */
		if (state == NULL)
		{
			state = makeJsonAggState(fcinfo);

			if( top_object )
				appendStringInfoChar(state, '{');  /* begin top-level json object */

			if(!PG_ARGISNULL(2)) /* output array json-name */
			{
				appendStringInfoQuotedString(state, PG_TEXT_DATUM_GET_CSTR( PG_GETARG_DATUM(2) ));
				appendStringInfoChar(state, ':');  /* array name delimiter */
			}
			appendStringInfoChar(state, '[');  /* array begin */
		}
		else
			appendStringInfoChar(state, ',');  /* delimiter */

		//call to serialize_record( ... )
		MemSet( &flinfo, 0, sizeof( flinfo ) );
		flinfo.fn_addr = serialize_record;
		flinfo.fn_nargs = 1;
		flinfo.fn_mcxt = fcinfo->flinfo->fn_mcxt;

		serialized_value = PG_TEXT_DATUM_GET_CSTR( FunctionCall1( &flinfo, PG_GETARG_DATUM(1) ) );

		appendStringInfoString(state, serialized_value);	  /* append value */
	}

	/*
	 * The transition type for json_agg() is declared to be "internal",
	 * which is a pass-by-value type the same size as a pointer.
	 */

	PG_RETURN_POINTER(state);
}

Datum json_agg_common_finalfn( PG_FUNCTION_ARGS, bool top_object )
{
	StringInfo state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	if (state != NULL)
	{
		appendStringInfoChar(state, ']');  /* array end */

		if( top_object )
			appendStringInfoChar(state, '}'); /* end top-level json object */

		PG_RETURN_TEXT_P(cstring_to_text(state->data));
	}
	else
	{
		if( top_object )
			PG_RETURN_TEXT_P( PG_CSTR_GET_TEXT( "{}" ) );
		else
			PG_RETURN_NULL();
	}
}

PG_FUNCTION_INFO_V1( json_agg_finalfn );
Datum json_agg_finalfn( PG_FUNCTION_ARGS )
{
	return json_agg_common_finalfn( fcinfo, true );
}

PG_FUNCTION_INFO_V1( json_agg_plain_finalfn );
Datum json_agg_plain_finalfn( PG_FUNCTION_ARGS )
{
	return json_agg_common_finalfn( fcinfo, false );
}

PG_FUNCTION_INFO_V1( json_agg_transfn );
Datum json_agg_transfn( PG_FUNCTION_ARGS )
{
	return json_agg_common_transfn( fcinfo, true );
}

PG_FUNCTION_INFO_V1( json_agg_plain_transfn );
Datum json_agg_plain_transfn( PG_FUNCTION_ARGS )
{
	return json_agg_common_transfn( fcinfo, false );
}
