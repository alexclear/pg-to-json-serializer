
/*
* @author Maxim Ky
* @date 2011-10-26
* @description pg-to-json-serializer common include file
*/

#include "utils/builtins.h"

#define PG_CSTR_GET_TEXT(cstrp) DatumGetTextP( DirectFunctionCall1(textin, CStringGetDatum(cstrp) ) )
#define PG_TEXT_GET_CSTR( textp ) DatumGetCString( DirectFunctionCall1(textout, PointerGetDatum(textp) ) )
#define PG_TEXT_DATUM_GET_CSTR( datum ) DatumGetCString( DirectFunctionCall1(textout, datum ) )

