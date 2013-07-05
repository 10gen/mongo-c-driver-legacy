/* test.c */

#include "test.h"
#include "mongo.h"
#include "bson_fast_macros.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#ifndef _WIN32
#include <sys/time.h>
#endif

/* supports preprocessor concatenation */
#define DB "benchmarks"

/* finds without indexes */
#define DO_SLOW_TESTS 1

#ifndef TEST_SERVER
#define TEST_SERVER "127.0.0.1"
#endif

#define PER_TRIAL 5000
#define BATCH_SIZE  100

static mongo conn[1];

/* strings used in the creation of the various sized documents by the make_x_fast variety of functions */
#define NAME1 "x"
#define NAME2 "integer"
#define NAME3 "number"
#define NAME4 "boolean"
#define NAME5 "array"
#define NAME6 "base_url"
#define NAME7 "total_word_count"
#define NAME8 "access_time"
#define NAME9 "meta_tags"
#define NAME10 "description"
#define NAME11 "author"
#define NAME12 "dynamically_created_meta_tag"
#define NAME13 "page_structure"
#define NAME14 "counted_tags"
#define NAME15 "no_of_js_attached"
#define NAME16 "no_of_images"
#define NAME17 "harvested_words"

#define ARR0 "0"
#define ARR1 "1"

#define STR1 "test"
#define STR2 "benchmark"
#define STR3 "http://www.example.com/test-me"
#define STR4 "i am a long description string"
#define STR5 "Holly Man"
#define STR6 "who know\n what"

#define N_WORDS 14
#define N_ARRAY_WORDS (N_WORDS * 20)
static const char *words[N_WORDS] = {
    "10gen","web","open","source","application","paas",
    "platform-as-a-service","technology","helps",
    "developers","focus","building","mongodb","mongo"
};

/* used by make_large_fast */
#define MAX_WORDS sizeof( "platform-as-a-service" )

static void make_small( bson *out, int i ) {
    bson_init( out );
    bson_append_new_oid( out, "_id" );
    bson_append_int( out, NAME1, i );
    bson_finish( out );
}

static void make_medium( bson *out, int i ) {
    bson_init( out );
    bson_append_new_oid( out, "_id" );
    bson_append_int( out, NAME1, i );
    bson_append_int( out, NAME2, 5 );
    bson_append_double( out, NAME3, 5.05 );
    bson_append_bool( out, NAME4, 0 );

    bson_append_start_array( out, NAME5 );
    bson_append_string( out, ARR0, STR1 );
    bson_append_string( out, ARR1, STR2 );
    bson_append_finish_object( out );

    bson_finish( out );
}

static void make_large( bson *out, int i ) {
    int num;
    char numstr[4];
    bson_init( out );

    bson_append_new_oid( out, "_id" );
    bson_append_int( out, NAME1, i );
    bson_append_string( out, NAME6, STR3 );
    bson_append_int( out, NAME7, 6743 );
    bson_append_int( out, NAME8, 999 ); /*TODO use date*/

    bson_append_start_object( out, NAME9 );
    bson_append_string( out, NAME10, STR4 );
    bson_append_string( out, NAME11, STR5 );
    bson_append_string( out, NAME12, STR6 );
    bson_append_finish_object( out );

    bson_append_start_object( out, NAME13 );
    bson_append_int( out, NAME14, 3450 );
    bson_append_int( out, NAME15, 10 );
    bson_append_int( out, NAME16, 6 );
    bson_append_finish_object( out );


    bson_append_start_array( out, NAME17 );
    for ( num=0; num < N_ARRAY_WORDS; num++ ) {
        bson_numstr( numstr, num );
        bson_append_string( out, numstr, words[num%N_WORDS] );
    }
    bson_append_finish_object( out );

    bson_finish( out );
}

static void serialize_small_test( void ) {
    int i;
    bson b;
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_small( &b, i );
        bson_destroy( &b );
    }
}
static void serialize_medium_test( void ) {
    int i;
    bson b;
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_medium( &b, i );
        bson_destroy( &b );
    }
}
static void serialize_large_test( void ) {
    int i;
    bson b;
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_large( &b, i );
        bson_destroy( &b );
    }
}

/* 
   for make_small_fast and make_medium_fast we assume fixed document sizes, we don't do this for 
   make_large_fast in order to provide an example of using the macros when we only know a maximum
   document length. 
*/

#define DOC_LENGTH_SMALL ( LEN_BEGIN_DOCUMENT()\
			+ LEN_WRITE_OID()\
			+ LEN_WRITE_INT32( sizeof(NAME1) )\
			+ LEN_CLOSE_DOCUMENT() )

/* 
   note the use of LEN_OPEN_ARRAY_INSIDE() when we are computing the total document length of the 
   array, this is the length that is passed to CLOSE_ARRAY_FIXED 
*/
#define ARR_LENGTH_MED ( LEN_OPEN_ARRAY_INSIDE()\
			+ LEN_WRITE_STRING( sizeof(ARR0), sizeof(STR1) )\
			+ LEN_WRITE_STRING( sizeof(ARR1), sizeof(STR2) )\
			+ LEN_CLOSE_ARRAY() )

/*
   note the use of LEN_OPEN_ARRAY_OUTSIDE() AND the already computed array length when we are computing the total 
   document length we need to be sure that we include LEN_OPEN_ARRAY_OUTSIDE and LEN_OPEN_ARRAY_INSIDE to
   correctly determine our size
*/
#define DOC_LENGTH_MEDIUM ( LEN_BEGIN_DOCUMENT()\
			+ LEN_WRITE_OID()\
			+ LEN_WRITE_INT32( sizeof(NAME1) )\
			+ LEN_WRITE_INT32( sizeof(NAME2) )\
			+ LEN_WRITE_DOUBLE( sizeof(NAME3) )\
			+ LEN_WRITE_BOOL( sizeof(NAME4) )\
			+ LEN_OPEN_ARRAY_OUTSIDE( sizeof(NAME5) )\
			+ ARR_LENGTH_MED\
			+ LEN_CLOSE_DOCUMENT() )

#define MAX_DOC_LENGTH_LARGE ( LEN_BEGIN_DOCUMENT()\
			+ LEN_WRITE_OID()\
			+ LEN_WRITE_INT32( sizeof(NAME1) )\
			+ LEN_WRITE_STRING( sizeof(NAME6), sizeof(STR3) )\
			+ LEN_WRITE_INT32( sizeof(NAME7) )\
			+ LEN_WRITE_INT32( sizeof(NAME8) )\
			+ LEN_OPEN_OBJECT_OUTSIDE( sizeof(NAME9) )\
			+ LEN_OPEN_OBJECT_INSIDE()\
			+ LEN_WRITE_STRING( sizeof(NAME10), sizeof(STR4) )\
			+ LEN_WRITE_STRING( sizeof(NAME11), sizeof(STR5) )\
			+ LEN_WRITE_STRING( sizeof(NAME11), sizeof(STR6) )\
			+ LEN_CLOSE_OBJECT()\
			+ LEN_OPEN_OBJECT_OUTSIDE( sizeof(NAME13) )\
			+ LEN_OPEN_OBJECT_INSIDE()\
			+ LEN_WRITE_INT32( sizeof(NAME14) )\
			+ LEN_WRITE_INT32( sizeof(NAME15) )\
			+ LEN_WRITE_INT32( sizeof(NAME16) )\
			+ LEN_CLOSE_OBJECT()\
			+ LEN_OPEN_ARRAY_OUTSIDE( sizeof(NAME17) )\
			+ LEN_OPEN_ARRAY_INSIDE()\
			+ LEN_WRITE_STRING(4, MAX_WORDS ) * N_ARRAY_WORDS\
                        + LEN_CLOSE_ARRAY()\
			+ LEN_CLOSE_DOCUMENT() )

static char* make_small_fast( char* p, int i ){
    bson_oid_t oid;

    BEGIN_DOCUMENT_FIXED( DOC_LENGTH_SMALL )
    WRITE_OID(&oid)
    WRITE_INT32( NAME1 ,sizeof(NAME1), i )
    CLOSE_DOCUMENT_FIXED()
    
    return p;
}

static char* make_medium_fast( char* p, int i ){
    bson_oid_t oid;

    BEGIN_DOCUMENT_FIXED(DOC_LENGTH_MEDIUM)
    WRITE_OID(&oid)
    WRITE_INT32( NAME1, sizeof(NAME1), i )
    WRITE_INT32( NAME2, sizeof(NAME2), 5 )
    WRITE_DOUBLE( NAME3, sizeof(NAME3), 5.05 )
    WRITE_BOOL(NAME4, sizeof(NAME4), 0 )

    OPEN_ARRAY_FIXED( NAME5, sizeof(NAME5), ARR_LENGTH_MED )
    WRITE_STRING( ARR0, sizeof(ARR0), STR1, sizeof(STR1) )
    WRITE_STRING( ARR1, sizeof(ARR1), STR2, sizeof(STR2) )
    CLOSE_ARRAY_FIXED()

    CLOSE_DOCUMENT_FIXED()

    return p;
}

static char* make_large_fast( char* p, int i ){
    int num;
    void *p_doclen, *p_objlen, *p_arrlen;
    int doclen = 0, objlen = 0, arrlen = 0;
    char numstr[4];
    bson_oid_t oid;

    doclen += LBEGIN_DOCUMENT(p_doclen)
    doclen += LWRITE_OID(&oid)
    doclen += LWRITE_INT32( NAME1, sizeof(NAME1), i )
    doclen += LWRITE_STRING( NAME6, sizeof(NAME6), STR3, sizeof(STR3) )
    doclen += LWRITE_INT32( NAME7, sizeof(NAME7), 6743 )
    doclen += LWRITE_INT32( NAME8, sizeof(NAME8), 999 )
 
    doclen += LEN_OPEN_OBJECT_OUTSIDE( sizeof(NAME9) );
    objlen = LEN_OPEN_OBJECT_INSIDE();
    OPEN_OBJECT(NAME9, sizeof(NAME9), p_objlen )
    objlen += LWRITE_STRING( NAME10, sizeof(NAME10), STR4, sizeof(STR4) )
    objlen += LWRITE_STRING( NAME11, sizeof(NAME11), STR5, sizeof(STR5) )
    objlen += LWRITE_STRING( NAME11, sizeof(NAME11), STR6, sizeof(STR6) )
    objlen += LCLOSE_OBJECT( p_objlen, objlen )
    doclen += objlen;
   
    doclen += LEN_OPEN_OBJECT_OUTSIDE( sizeof(NAME13) );
    objlen = LEN_OPEN_OBJECT_INSIDE();
    OPEN_OBJECT(NAME13, sizeof(NAME13), p_objlen )
    objlen += LWRITE_INT32( NAME14, sizeof(NAME14), 3450 )
    objlen += LWRITE_INT32( NAME15, sizeof(NAME15), 10 )
    objlen += LWRITE_INT32( NAME16, sizeof(NAME16), 6 )
    objlen += LCLOSE_OBJECT( p_objlen, objlen )
    doclen += objlen;

    doclen += LEN_OPEN_ARRAY_OUTSIDE( sizeof(NAME17) );
    arrlen = LEN_OPEN_ARRAY_INSIDE();
    OPEN_ARRAY(NAME17, sizeof(NAME17), p_arrlen )

    for ( num=0; num < N_ARRAY_WORDS; num++ ){
        bson_numstr( numstr, num );
        arrlen += LWRITE_STRING( numstr, strlen(numstr)+1, words[num%N_WORDS], strlen( words[num%N_WORDS] ) + 1 )
    }
    arrlen += LCLOSE_ARRAY(p_arrlen, arrlen)
    doclen += arrlen;

    doclen += LCLOSE_DOCUMENT( p_doclen, doclen )

    return p;
}

static void serialize_small_test_fast( void ) {
    int i;
    char p[DOC_LENGTH_SMALL];

    for ( i=0; i<PER_TRIAL; i++ ) {
        make_small_fast( p, i );
    }
}
static void serialize_medium_test_fast( void ) {
    int i;
    char p[DOC_LENGTH_MEDIUM];

    for ( i=0; i<PER_TRIAL; i++ ) {
        make_medium_fast( p, i );
    }
}
static void serialize_large_test_fast( void ) {
    int i;
    char p[MAX_DOC_LENGTH_LARGE]; 

    for ( i=0; i<PER_TRIAL; i++ ) {
        make_large_fast( p, i );
    }

}

#define COL_BATCHSMALLFAST DB ".batch.small.fast"
static void batch_insert_small_test_fast( void ) {
    int i, j;
    int len = (BATCH_SIZE * DOC_LENGTH_SMALL) + FAST_HEADER_LENGTH( COL_BATCHSMALLFAST ); 	
    char base[len], *p, *p_base = &base[0];

    for ( i=0; i < ( PER_TRIAL / BATCH_SIZE ); i++ ) {

	p = base;
	p = fast_prepare_insertheader( p, COL_BATCHSMALLFAST, 0 );

        for ( j=0; j < BATCH_SIZE; j++ )
	    p = make_small_fast( p, i );

	WRITE_LENGTH( p_base, p - p_base )

        mongo_fast_with_write_concern( conn, COL_BATCHSMALLFAST, p_base, p - p_base, NULL );
    }
}

#define COL_BATCHMEDIUMFAST DB ".batch.medium.fast"
static void batch_insert_medium_test_fast( void ) {
    int i, j;
    int len = (BATCH_SIZE * DOC_LENGTH_MEDIUM) + FAST_HEADER_LENGTH( COL_BATCHMEDIUMFAST );
    char *base = malloc( len ), *p;
    if (!base){
	fprintf(stderr,"oom\n");
	exit(-1);
    }

    for ( i=0; i < ( PER_TRIAL / BATCH_SIZE ); i++ ) {
	p = base;
	p = fast_prepare_insertheader( p, COL_BATCHMEDIUMFAST, 0 );

        for ( j=0; j < BATCH_SIZE; j++ )
	    p = make_medium_fast( p, i );

	WRITE_LENGTH(base, p - base)

        mongo_fast_with_write_concern( conn, COL_BATCHMEDIUMFAST, base, p - base, NULL );
    }
    free(base);
}

#define COL_BATCHLARGEFAST DB ".batch.large.fast"
static void batch_insert_large_test_fast( void ) {
    int i, j;
    int len = (BATCH_SIZE * MAX_DOC_LENGTH_LARGE) + FAST_HEADER_LENGTH( COL_BATCHLARGEFAST );
    char *base = malloc( len ), *p;
    if (!base){
	fprintf(stderr,"oom\n");
	exit(-1);
    }

    for ( i=0; i < ( PER_TRIAL / BATCH_SIZE ); i++ ) {
	p = base;
	p = fast_prepare_insertheader( p, COL_BATCHLARGEFAST, 0 );	

        for ( j=0; j < BATCH_SIZE; j++ )
	    p = make_large_fast( p, i );

	WRITE_LENGTH(base, p - base)

        mongo_fast_with_write_concern( conn, COL_BATCHLARGEFAST, base, p - base, NULL );
    }
    free(base);
}

static void single_insert_small_test( void ) {
    int i;
    bson b;
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_small( &b, i );
        mongo_insert( conn, DB ".single.small", &b, NULL );
        bson_destroy( &b );
    }
}

static void single_insert_medium_test( void ) {
    int i;
    bson b;
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_medium( &b, i );
        mongo_insert( conn, DB ".single.medium", &b, NULL );
        bson_destroy( &b );
    }
}

static void single_insert_large_test( void ) {
    int i;
    bson b;
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_large( &b, i );
        mongo_insert( conn, DB ".single.large", &b, NULL );
        bson_destroy( &b );
    }
}

static void index_insert_small_test( void ) {
    int i;
    bson b;
    ASSERT( mongo_create_simple_index( conn, DB ".index.small", "x", 0, NULL ) == MONGO_OK );
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_small( &b, i );
        mongo_insert( conn, DB ".index.small", &b, NULL );
        bson_destroy( &b );
    }
}

static void index_insert_medium_test( void ) {
    int i;
    bson b;
    ASSERT( mongo_create_simple_index( conn, DB ".index.medium", "x", 0, NULL ) == MONGO_OK );
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_medium( &b, i );
        mongo_insert( conn, DB ".index.medium", &b, NULL );
        bson_destroy( &b );
    }
}

static void index_insert_large_test( void ) {
    int i;
    bson b;
    ASSERT( mongo_create_simple_index( conn, DB ".index.large", "x", 0, NULL ) == MONGO_OK );
    for ( i=0; i<PER_TRIAL; i++ ) {
        make_large( &b, i );
        mongo_insert( conn, DB ".index.large", &b, NULL );
        bson_destroy( &b );
    }
}

static void batch_insert_small_test( void ) {
    int i, j;
    bson b[BATCH_SIZE];
    const bson *bp[BATCH_SIZE];
    for ( j=0; j < BATCH_SIZE; j++ )
        bp[j] = &b[j];

    for ( i=0; i < ( PER_TRIAL / BATCH_SIZE ); i++ ) {
        for ( j=0; j < BATCH_SIZE; j++ )
            make_small( &b[j], i );

        mongo_insert_batch( conn, DB ".batch.small", bp, BATCH_SIZE, NULL, 0 );

        for ( j=0; j < BATCH_SIZE; j++ )
            bson_destroy( &b[j] );
    }
}

static void batch_insert_medium_test( void ) {
    int i, j;
    bson b[BATCH_SIZE];
    const bson *bp[BATCH_SIZE];
    for ( j=0; j < BATCH_SIZE; j++ )
        bp[j] = &b[j];

    for ( i=0; i < ( PER_TRIAL / BATCH_SIZE ); i++ ) {
        for ( j=0; j < BATCH_SIZE; j++ )
            make_medium( &b[j], i );

        mongo_insert_batch( conn, DB ".batch.medium", bp, BATCH_SIZE, NULL, 0 );

        for ( j=0; j < BATCH_SIZE; j++ )
            bson_destroy( &b[j] );
    }
}

static void batch_insert_large_test( void ) {
    int i, j;
    bson b[BATCH_SIZE];
    const bson *bp[BATCH_SIZE];
    for ( j=0; j < BATCH_SIZE; j++ )
        bp[j] = &b[j];

    for ( i=0; i < ( PER_TRIAL / BATCH_SIZE ); i++ ) {
        for ( j=0; j < BATCH_SIZE; j++ )
            make_large( &b[j], i );

        mongo_insert_batch( conn, DB ".batch.large", bp, BATCH_SIZE, NULL, 0 );

        for ( j=0; j < BATCH_SIZE; j++ )
            bson_destroy( &b[j] );
    }
}

static void make_query( bson *b ) {
    bson_init( b );
    bson_append_int( b, "x", PER_TRIAL/2 );
    bson_finish( b );
}

static void find_one( const char *ns ) {
    bson b;
    int i;
    for ( i=0; i < PER_TRIAL; i++ ) {
        make_query( &b );
        ASSERT( mongo_find_one( conn, ns, &b, NULL, NULL ) == MONGO_OK );
        bson_destroy( &b );
    }
}

static void find_one_noindex_small_test( void )  {
    find_one( DB ".single.small" );
}
static void find_one_noindex_medium_test( void ) {
    find_one( DB ".single.medium" );
}
static void find_one_noindex_large_test( void )  {
    find_one( DB ".single.large" );
}

static void find_one_index_small_test( void )  {
    find_one( DB ".index.small" );
}
static void find_one_index_medium_test( void ) {
    find_one( DB ".index.medium" );
}
static void find_one_index_large_test( void )  {
    find_one( DB ".index.large" );
}

static void find( const char *ns ) {
    bson b;
    int i;
    for ( i=0; i < PER_TRIAL; i++ ) {
        mongo_cursor *cursor;
        make_query( &b );
        cursor = mongo_find( conn, ns, &b, NULL, 0,0,0 );
        ASSERT( cursor );

        while( mongo_cursor_next( cursor ) == MONGO_OK )
            {}

        mongo_cursor_destroy( cursor );
        bson_destroy( &b );
    }
}

static void find_noindex_small_test( void )  {
    find( DB ".single.small" );
}
static void find_noindex_medium_test( void ) {
    find( DB ".single.medium" );
}
static void find_noindex_large_test( void )  {
    find( DB ".single.large" );
}

static void find_index_small_test( void )  {
    find( DB ".index.small" );
}
static void find_index_medium_test( void ) {
    find( DB ".index.medium" );
}
static void find_index_large_test( void )  {
    find( DB ".index.large" );
}


static void find_range( const char *ns ) {
    int i;
    bson bb;
    mongo_cursor *cursor;

    for ( i=0; i < PER_TRIAL; i++ ) {
        int j=0;

        bson_init( &bb );
        bson_append_start_object( &bb, "x" );
        bson_append_int( &bb, "$gt", PER_TRIAL/2 );
        bson_append_int( &bb, "$lt", PER_TRIAL/2 + BATCH_SIZE );
        bson_append_finish_object( &bb );
        bson_finish( &bb );

        cursor = mongo_find( conn, ns, &bb, NULL, 0,0,0 );
        ASSERT( cursor );

        while( mongo_cursor_next( cursor ) == MONGO_OK ) {
            j++;
        }
        ASSERT( j == BATCH_SIZE-1 );

        mongo_cursor_destroy( cursor );
        bson_destroy( &bb );
    }
}

static void find_range_small_test( void )  {
    find_range( DB ".index.small" );
}
static void find_range_medium_test( void ) {
    find_range( DB ".index.medium" );
}
static void find_range_large_test( void )  {
    find_range( DB ".index.large" );
}

typedef void( *nullary )( void );
static void time_it( nullary func, const char *name, bson_bool_t gle ) {
    double timer;
    double ops;

#ifdef _WIN32
    int64_t start, end;

    start = GetTickCount64();
    func();
    if ( gle ) ASSERT( !mongo_cmd_get_last_error( conn, DB, NULL ) );
    end = GetTickCount64();

    timer = end - start;
#else
    struct timeval start, end;

    gettimeofday( &start, NULL );
    func();
    gettimeofday( &end, NULL );
    if ( gle ) ASSERT( !mongo_cmd_get_last_error( conn, DB, NULL ) );

    timer = end.tv_sec - start.tv_sec;
    timer *= 1000000;
    timer += end.tv_usec - start.tv_usec;
#endif

    ops = PER_TRIAL / timer;
    ops *= 1000000;

    printf( "%-45s\t%15f\n", name, ops );
}

#define TIME(func, gle) (time_it(func, #func, gle));

static void clean( void ) {
    if ( mongo_cmd_drop_db( conn, DB ) != MONGO_OK ) {
        printf( "failed to drop db\n" );
        exit( 1 );
    }

    /* create the db */
    mongo_insert( conn, DB ".creation", bson_shared_empty( ), NULL );
    ASSERT( !mongo_cmd_get_last_error( conn, DB, NULL ) );
}

int main() {
    INIT_SOCKETS_FOR_WINDOWS;
    CONN_CLIENT_TEST;

    /*if( mongo_client( conn,"/tmp/mongodb-27017.sock", -1 ) != MONGO_OK ) { 
        printf( "Failed to connect" ); 
        exit( 1 ); 
    }*/

    clean();

    printf( "-----\n" );
    TIME( serialize_small_test, 0 );
    TIME( serialize_medium_test, 0 );
    TIME( serialize_large_test, 0 );

    printf( "-----\n" );
    TIME( serialize_small_test_fast, 0 );
    TIME( serialize_medium_test_fast, 0 );
    TIME( serialize_large_test_fast, 0 );

    printf( "-----\n" );
    TIME( single_insert_small_test, 1 );
    TIME( single_insert_medium_test, 1 );
    TIME( single_insert_large_test, 1 );

    printf( "-----\n" );
    TIME( index_insert_small_test, 1 );
    TIME( index_insert_medium_test, 1 );
    TIME( index_insert_large_test, 1 );

    printf( "-----\n" );
    TIME( batch_insert_small_test_fast, 1 );
    TIME( batch_insert_medium_test_fast, 1 );
    TIME( batch_insert_large_test_fast, 1 );

    printf( "-----\n" );
    TIME( batch_insert_small_test, 1 );
    TIME( batch_insert_medium_test, 1 );
    TIME( batch_insert_large_test, 1 );

#if DO_SLOW_TESTS
    printf( "-----\n" );
    TIME( find_one_noindex_small_test, 0 );
    TIME( find_one_noindex_medium_test, 0 );
    TIME( find_one_noindex_large_test, 0 );
#endif

    printf( "-----\n" );
    TIME( find_one_index_small_test, 0 );
    TIME( find_one_index_medium_test, 0 );
    TIME( find_one_index_large_test, 0 );

#if DO_SLOW_TESTS
    printf( "-----\n" );
    TIME( find_noindex_small_test, 0 );
    TIME( find_noindex_medium_test, 0 );
    TIME( find_noindex_large_test, 0 );
#endif

    printf( "-----\n" );
    TIME( find_index_small_test, 0 );
    TIME( find_index_medium_test, 0 );
    TIME( find_index_large_test, 0 );

    printf( "-----\n" );
    TIME( find_range_small_test, 0 );
    TIME( find_range_medium_test, 0 );
    TIME( find_range_large_test, 0 );


    mongo_destroy( conn );

    return 0;
}
