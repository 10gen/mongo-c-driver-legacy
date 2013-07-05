/*
  fast_test.c

  verifying operation of bson_fast_macros.h macros and mongo_fast_with_write_concern()
*/

#ifndef TEST_SERVER
#define TEST_SERVER "127.0.0.1"
#endif

#include "test.h"
#include "mongo.h"
#include "bson_fast_macros.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#ifndef M_SQRT2
#define M_SQRT2 1.41421356237309504880 
#endif

const char *db = "test";
const char *col = "fast";
const char *ns = "test.fast";

#define N_ITERATIONS 10
static const char *word_set1[N_ITERATIONS] = {
    "river", "lake", "stream", "ocean", "sea", "forest", "woods", "fields", "mountain", "valley"
};
static const char *word_set2[N_ITERATIONS] = {
    "planes", "trains", "automobiles", "buses", "lorries", "vans", "bicycles", "hovercraft", "ship", "airship"
};
static const char *word_set3[N_ITERATIONS] = {
    "mercury", "venus", "Earth", "mars", "jupiter", "saturn", "uranus", "neptune", "pluto", "ceres"
};

int main() {
    mongo conn[1];
    mongo_cursor cursor[1];

    int i,n;
    bson_oid_t oid[N_ITERATIONS];
    char numstr[4], _base[10240], *p, *base = &_base[0];
    void *p_doclen, *p_objlen, *p_arrlen;
    int doclen = 0, objlen = 0, arrlen = 0, total_len;

    INIT_SOCKETS_FOR_WINDOWS;
    CONN_CLIENT_TEST;

    mongo_cmd_drop_collection( conn, db, col, NULL );
    mongo_find_one( conn, ns, bson_shared_empty( ), bson_shared_empty( ), NULL );

    total_len = FAST_HEADER_LENGTH(ns);
    p = fast_prepare_insertheader( base, ns, 0 );

    for( i=0; i< N_ITERATIONS; i++ ) {
        doclen = LBEGIN_DOCUMENT(p_doclen)
        doclen += LWRITE_OID(&oid[i])
        doclen += LWRITE_STRING( "string", sizeof("string"), "a short little string", sizeof("a short little string") )
        doclen += LWRITE_INT32( "integer", sizeof("integer"), 1337 )
	doclen += LWRITE_BOOL( "bool_true", sizeof("bool_true"), 1 )
        doclen += LWRITE_BOOL( "bool_false", sizeof("bool_false"), 0 )
	doclen += LWRITE_DOUBLE( "double", sizeof("double"), M_SQRT2 )
 
        doclen += LEN_OPEN_OBJECT_OUTSIDE( sizeof("objectA") );
        objlen = LEN_OPEN_OBJECT_INSIDE();
        OPEN_OBJECT("objectA", sizeof("objectA"), p_objlen )
        objlen += LWRITE_STRING( "nature", sizeof("nature"), word_set1[i], strlen(word_set1[i])+1 )
        objlen += LWRITE_STRING( "vehicle", sizeof("vehicle"), word_set2[i], strlen(word_set2[i])+1 )
        objlen += LWRITE_STRING( "solar system", sizeof("solar system"), word_set3[i], strlen(word_set3[i])+1 )
        objlen += LCLOSE_OBJECT( p_objlen, objlen )
        doclen += objlen;
    
        doclen += LEN_OPEN_OBJECT_OUTSIDE( sizeof("objectB") );
        objlen = LEN_OPEN_OBJECT_INSIDE();
        OPEN_OBJECT("objectB", sizeof("objectB"), p_objlen )
        objlen += LWRITE_INT32( "i*i", sizeof("i*i"), (i*i) )
        objlen += LWRITE_INT32( "i cubed", sizeof("i cubed"), (i*i*i) )
        objlen += LWRITE_DOUBLE( "i*SQRT2", sizeof("sqrt(i)"), (i*M_SQRT2) )
        objlen += LCLOSE_OBJECT( p_objlen, objlen )
        doclen += objlen;

        doclen += LEN_OPEN_ARRAY_OUTSIDE( sizeof("array") );
        arrlen = LEN_OPEN_ARRAY_INSIDE();
        OPEN_ARRAY("array", sizeof("array"), p_arrlen )

        for ( n=0; n < 30; n++ ){
            bson_numstr( numstr, n );
            arrlen += LWRITE_DOUBLE( numstr, strlen(numstr)+1, n*n*M_PI )
        }
        arrlen += LCLOSE_ARRAY(p_arrlen, arrlen)
	doclen += arrlen;
	
	doclen += LCLOSE_DOCUMENT( p_doclen, doclen )

	total_len += doclen;
    }

    WRITE_LENGTH( base, total_len )
    ASSERT( p - base == total_len );
    ASSERT( mongo_fast_with_write_concern( conn, ns, base, p-base, NULL ) == MONGO_OK );
    ASSERT( !mongo_cmd_get_last_error( conn, db, NULL ) );

    mongo_cursor_init( cursor, conn, ns );

    i = 0;
    while( mongo_cursor_next( cursor ) == MONGO_OK ) {

        bson_iterator it, sub_it;
        bson_iterator_init( &it, mongo_cursor_bson( cursor ) );

	ASSERT( bson_iterator_next(&it) == BSON_OID );
	ASSERT( strcmp( bson_iterator_key(&it), "_id" ) == 0 );
	ASSERT( memcmp( bson_iterator_oid(&it), &oid[i], sizeof(bson_oid_t) ) == 0 );

	ASSERT( bson_iterator_next(&it) == BSON_STRING );
	ASSERT( strcmp( bson_iterator_key(&it), "string" ) == 0);
	ASSERT( strcmp( bson_iterator_string(&it), "a short little string") == 0);

	ASSERT( bson_iterator_next(&it) == BSON_INT );
	ASSERT( strcmp( bson_iterator_key(&it), "integer" ) == 0 );
	ASSERT( bson_iterator_int(&it) == 1337 );

	ASSERT( bson_iterator_next(&it) == BSON_BOOL );
	ASSERT( strcmp( bson_iterator_key(&it), "bool_true" ) == 0);
	ASSERT( bson_iterator_bool(&it) );

	ASSERT( bson_iterator_next(&it) == BSON_BOOL );
	ASSERT( strcmp( bson_iterator_key(&it), "bool_false" ) == 0);
	ASSERT( !bson_iterator_bool(&it) );

	ASSERT( bson_iterator_next(&it) == BSON_DOUBLE );
	ASSERT( strcmp( bson_iterator_key(&it), "double" ) == 0);
	ASSERT( bson_iterator_double(&it) == M_SQRT2 );
	
	ASSERT( bson_iterator_next(&it) == BSON_OBJECT );
	ASSERT( strcmp( bson_iterator_key(&it), "objectA" ) == 0);	
	bson_iterator_subiterator( &it, &sub_it );	

	   ASSERT( bson_iterator_next(&sub_it) == BSON_STRING );
	   ASSERT( strcmp( bson_iterator_key(&sub_it), "nature" ) == 0 );
	   ASSERT( strcmp( bson_iterator_string(&sub_it), word_set1[i] ) == 0);
	
	   ASSERT( bson_iterator_next(&sub_it) == BSON_STRING );
	   ASSERT( strcmp( bson_iterator_key(&sub_it), "vehicle" ) == 0);
	   ASSERT( strcmp( bson_iterator_string(&sub_it), word_set2[i] ) == 0);

	   ASSERT( bson_iterator_next(&sub_it) == BSON_STRING );
	   ASSERT( strcmp( bson_iterator_key(&sub_it), "solar system" ) == 0 );
	   ASSERT( strcmp( bson_iterator_string(&sub_it), word_set3[i] ) == 0);
	
           ASSERT( !bson_iterator_next(&sub_it) );

	ASSERT( bson_iterator_next(&it) == BSON_OBJECT );
	ASSERT( strcmp( bson_iterator_key(&it), "objectB" ) == 0 );	
	bson_iterator_subiterator( &it, &sub_it );
 
	   ASSERT( bson_iterator_next(&sub_it) == BSON_INT );
   	   ASSERT( strcmp( bson_iterator_key(&sub_it), "i*i" ) == 0 );
	   ASSERT( bson_iterator_int(&sub_it) == (i*i) );

	   ASSERT( bson_iterator_next(&sub_it) == BSON_INT );
   	   ASSERT( strcmp( bson_iterator_key(&sub_it), "i cubed" ) == 0 );
	   ASSERT( bson_iterator_int(&sub_it) == (i*i*i) );

	   ASSERT( bson_iterator_next(&sub_it) == BSON_DOUBLE );
	   ASSERT( strcmp( bson_iterator_key(&sub_it), "i*SQRT2" ) == 0 );
	   ASSERT( bson_iterator_double(&sub_it) == (i*M_SQRT2) );

           ASSERT( !bson_iterator_next(&sub_it) );

	ASSERT( bson_iterator_next(&it) == BSON_ARRAY );
	ASSERT( strcmp( bson_iterator_key(&it), "array" ) == 0 );	
	bson_iterator_subiterator( &it, &sub_it );

	for (n = 0; n < 30; n++ ){
	   bson_numstr( numstr, n );

	   ASSERT( bson_iterator_next(&sub_it) == BSON_DOUBLE );
	   ASSERT( strcmp( bson_iterator_key(&sub_it), numstr ) == 0 );
	   ASSERT( bson_iterator_double(&sub_it) == (n*n*M_PI) );	
	}

        ASSERT( !bson_iterator_next(&sub_it) );
        ASSERT( !bson_iterator_next(&it) );
	i++;
    }

    mongo_cursor_destroy( cursor );
    ASSERT( mongo_cmd_drop_db( conn, "test" ) == MONGO_OK );
    mongo_disconnect( conn );
    mongo_destroy( conn );

    return 0;
}
