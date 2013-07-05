#ifndef _BSON_FAST_MACROS_H
#define _BSON_FAST_MACROS_H

#include "mongo.h"
#include "bson.h"

#ifdef MONGO_BIG_ENDIAN
#error "Big Endian not yet implemented"
#else
#define write_le_int32( VAL, P ) (*( (int32_t*) P )) = (int32_t) (VAL);
#define write_le_double( DBL, P ) (*( (double*) p )) = (double) (DBL);
#endif

/*****************************************************************************************

Macros for rapidly generating BSON documents. This file is included in the 'test' directory
as, whilst useful, only a few component types are currently implemented. 

At the time of writing 27/06/13 these include, arrays/objects/string/int32/double/bool/oid.

All macros depend on a pointer 'char* p' being defined and being used as a pointer to the 
current position in the document buffer.

Most macro sets <NAME> have a <NAME>, LEN_<NAME> and L_<NAME> variety, these varieties offer
enough flexibility such that BSON documents can be created when we either know the expected 
length or max length of the document or if we don't and need to grow a buffer as we write the 
document out.

<NAME> varieties take a given input and write to the buffer 'p' the necessary bytes to represent
  that input in the BSON document. The pointer 'p' is updated to the next unused byte in the buffer.

LEN_<NAME> varieties return the buffer space required for its LEN_<NAME> counterpart, these
  can be used to determine how much space in the buffer a new component requires before writing 
  the data. (nb. please pay special attention to lengths when using OPEN/CLOSE ARRAY/OBJECT)

L_<NAME> varieties combine <NAME> and LEN_<NAME> macros to allow a clean and tidy way to keep
  track of the length of data written. See benchmark_test.c for examples.

All macros with a string input require the associated length (for example NAME_LEN, STR_LEN) 
to be inclusive of the null character.

******************************************************************************************/

/* BEGIN/END A DOCUMENT OF KNOWN LENGTH, LEN */
#define LEN_BEGIN_DOCUMENT() 4
#define BEGIN_DOCUMENT_FIXED(LEN) \
    write_le_int32( LEN, p );\
    p += 4;
#define LBEGIN_DOCUMENT_FIXED(LEN) \
    LEN_BEGIN_DOCUMENT();\
    BEGIN_DOCUMENT_FIXED(LEN)

#define LEN_CLOSE_DOCUMENT() 1
#define CLOSE_DOCUMENT_FIXED()\
    (*p++) = 0;
#define LCLOSE_DOCUMENT_FIXED()\
    LEN_CLOSE_DOCUMENT();\
    CLOSE_DOCUMENT_FIXED()

/*
  BEGIN/END A DOCUMENT OF UNKNOWN LENGTH

  P_DOCLEN is a void* and holds the location where the document length is to be written, pass
    the same variable to BEGIN and CLOSE, do not modify the variable between the BEGIN and CLOSE calls
  LEN is cast to an int32, this is the total length of the document and is the sum of all BSON components as reported 
    by the relevant LEN_ functions 
*/
#define BEGIN_DOCUMENT(P_DOCLEN)\
    P_DOCLEN = p;\
    p+= 4;
#define LBEGIN_DOCUMENT(P_DOCLEN)\
    LEN_BEGIN_DOCUMENT();\
    BEGIN_DOCUMENT(P_DOCLEN)

#define CLOSE_DOCUMENT(P_DOCLEN, LEN)\
    write_le_int32( LEN, P_DOCLEN );\
    (*p++) = 0;
#define LCLOSE_DOCUMENT(P_DOCLEN, LEN)\
    LEN_CLOSE_DOCUMENT();\
    CLOSE_DOCUMENT(P_DOCLEN,LEN)


/*
  OPEN/CLOSE ARRAY/OBJECT LENGTHS

  Be careful that you correctly account for lengths when using arrays and objects, the total length consumed by OPEN
  is the sum of both OUTSIDE and INSIDE.

  The OUTSIDE length is not to be added to the computed length of the array/object as passed in via ARRLEN/OBJLEN.
  The INSIDE length however must be added to the computed length passed in via ARRLEN/OBJLEN.

  nb. open array/objects do not have an L form
*/ 

/* Array or Object LEN */
#define LEN_OPEN_ARROBJ_OUTSIDE(NAME_LEN) (1 + NAME_LEN)
#define LEN_OPEN_ARROBJ_INSIDE() LEN_BEGIN_DOCUMENT()

/* OPEN/CLOSE AN ARRAY/OBJECT OF KNOWN LENGTH, LEN */
#define OPEN_ARROBJ_FIXED( NAME, NAME_LEN, LEN, TYPE )\
    (*p++) = TYPE;\
    memcpy(p,NAME,NAME_LEN);\
    p+=NAME_LEN;\
    BEGIN_DOCUMENT_FIXED(LEN)

#define LEN_CLOSE_ARROBJ() LEN_CLOSE_DOCUMENT()
#define CLOSE_ARROBJ_FIXED() CLOSE_DOCUMENT_FIXED()
#define LCLOSE_ARROBJ_FIXED()\
    LEN_CLOSE_ARROBJ();\
    CLOSE_ARROBJ_FIXED()

/* OPEN/CLOSE AN ARRAY/OBJECT OF UNKNOWN LENGTH (SAME PROCEDURE AS WITH DOCUMENT) */
#define OPEN_ARROBJ( NAME, NAME_LEN, P_LEN, TYPE )\
    (*p++) = TYPE;\
    memcpy(p,NAME,NAME_LEN);\
    p+=NAME_LEN;\
    BEGIN_DOCUMENT(P_LEN)

#define CLOSE_ARROBJ(P_LEN, LEN) CLOSE_DOCUMENT(P_LEN,LEN)
#define LCLOSE_ARROBJ(P_LEN, LEN)\
    LEN_CLOSE_ARROBJ();\
    CLOSE_ARROBJ(P_LEN, LEN )

/* Array pseudonyms */
#define LEN_OPEN_ARRAY_OUTSIDE( NAME_LEN ) LEN_OPEN_ARROBJ_OUTSIDE(NAME_LEN)
#define LEN_OPEN_ARRAY_INSIDE() LEN_OPEN_ARROBJ_INSIDE()
#define OPEN_ARRAY_FIXED( NAME, NAME_LEN, ARRLEN ) OPEN_ARROBJ_FIXED(NAME,NAME_LEN,ARRLEN,0x4)
#define OPEN_ARRAY( NAME, NAME_LEN, P_ARRLEN ) OPEN_ARROBJ(NAME,NAME_LEN,P_ARRLEN,0x4)
#define LEN_CLOSE_ARRAY() LEN_CLOSE_ARROBJ()
#define CLOSE_ARRAY_FIXED() CLOSE_ARROBJ_FIXED()
#define CLOSE_ARRAY(P_ARRLEN, ARRLEN ) CLOSE_ARROBJ(P_ARRLEN, ARRLEN )
#define LCLOSE_ARRAY_FIXED() LCLOSE_ARROBJ_FIXED()
#define LCLOSE_ARRAY(P_ARRLEN, ARRLEN ) LCLOSE_ARROBJ(P_ARRLEN, ARRLEN )

/* Object pseudonyms */
#define LEN_OPEN_OBJECT_OUTSIDE( NAME_LEN ) 1 + NAME_LEN
#define LEN_OPEN_OBJECT_INSIDE() LEN_OPEN_ARROBJ_INSIDE()
#define OPEN_OBJECT_FIXED( NAME, NAME_LEN, OBJLEN ) OPEN_ARROBJ_FIXED(NAME,NAME_LEN,OBJLEN,0x3)
#define OPEN_OBJECT( NAME, NAME_LEN, P_OBJLEN ) OPEN_ARROBJ(NAME,NAME_LEN,P_OBJLEN,0x3)
#define LEN_CLOSE_OBJECT() LEN_CLOSE_ARROBJ()
#define CLOSE_OBJECT_FIXED() CLOSE_ARROBJ_FIXED()
#define CLOSE_OBJECT(P_OBJLEN, OBJLEN ) CLOSE_ARROBJ(P_OBJLEN, OBJLEN )
#define LCLOSE_OBJECT_FIXED() LCLOSE_ARROBJ_FIXED()
#define LCLOSE_OBJECT(P_OBJLEN, OBJLEN ) LCLOSE_ARROBJ(P_OBJLEN, OBJLEN )

/*
  OID

  P_OID is (bson_oid_t*)
*/
#define LEN_WRITE_OID() (5 + sizeof(bson_oid_t))
#define WRITE_OID(P_OID)\
    bson_oid_gen(P_OID);\
    (*p++) = 0x7;\
    memcpy(p, "_id\0", 4 );\
    p+=4;\
    memcpy(p, P_OID, sizeof(bson_oid_t) );\
    p+=sizeof(bson_oid_t);
#define LWRITE_OID(P_OID)\
    LEN_WRITE_OID();\
    WRITE_OID(P_OID);

/* INT32 */
#define LEN_WRITE_INT32(NAME_LEN) (5 + NAME_LEN)
#define WRITE_INT32(NAME, NAME_LEN, INT32)\
    (*p++) = 0x10;\
    memcpy(p,NAME,NAME_LEN);\
    p+=NAME_LEN;\
    write_le_int32( INT32, p );\
    p+= 4;
#define LWRITE_INT32(NAME, NAME_LEN, INT32)\
    LEN_WRITE_INT32(NAME_LEN);\
    WRITE_INT32(NAME,NAME_LEN,INT32)

/* DOUBLE */
#define LEN_WRITE_DOUBLE(NAME_LEN) (9 + NAME_LEN)
#define WRITE_DOUBLE(NAME, NAME_LEN, DBL)\
    (*p++) = 0x1;\
    memcpy(p,NAME,NAME_LEN);\
    p+=NAME_LEN;\
    write_le_double(DBL, p );\
    p+= 8;
#define LWRITE_DOUBLE(NAME, NAME_LEN, DBL)\
    LEN_WRITE_DOUBLE(NAME_LEN);\
    WRITE_DOUBLE(NAME,NAME_LEN,DBL)

/* BOOL */
#define LEN_WRITE_BOOL(NAME_LEN) (2 + NAME_LEN)
#define WRITE_BOOL(NAME, NAME_LEN, VAL)\
    (*p++) = 0x8;\
    memcpy(p,NAME,NAME_LEN);\
    p+=NAME_LEN;\
    (*p++) = ( VAL ) ? 0x1 : 0x0;
#define LWRITE_BOOL(NAME, NAME_LEN, VAL)\
    LEN_WRITE_BOOL(NAME_LEN);\
    WRITE_BOOL(NAME,NAME_LEN,VAL)

/* STRING */
#define LEN_WRITE_STRING(NAME_LEN, STR_LEN) (5 + NAME_LEN + STR_LEN)
#define WRITE_STRING(NAME, NAME_LEN, STR, STR_LEN )\
    (*p++) = 0x2;\
    memcpy(p,NAME,NAME_LEN);\
    p+=NAME_LEN;\
    write_le_int32( STR_LEN, p );\
    p+=4;\
    memcpy(p, STR, STR_LEN);\
    p+=STR_LEN;
#define LWRITE_STRING(NAME, NAME_LEN, STR, STR_LEN )\
    LEN_WRITE_STRING(NAME_LEN, STR_LEN );\
    WRITE_STRING(NAME,NAME_LEN,STR,STR_LEN )


/*
  Header preparation routine for batch inserts, this sets up the necessary header information when sending
  a message to mongodb.

  the total size of the message including documents must also include the header component, the total size including
  all documents must be written to the first int32 of the message to be sent to the server. This can be done by passing
  WRITE_LENGTH a pointer to the start of the message.

  see benchmark_test.c for examples of sending messages to mongodb via mongo_fast_with_write_concern()
*/
#define FAST_HEADER_LENGTH(COLNAME) strlen(COLNAME) + 1 + sizeof(mongo_header) + sizeof(int32_t)
#define WRITE_LENGTH( BASE, LEN ) write_le_int32( LEN, BASE );
static inline char* fast_prepare_insertheader( char* p, const char* ns, int continue_on_err ){
    mongo_header *h = (mongo_header*) p;
    int len;
   
    h->id = 0;
    h->responseTo = 0; 
    write_le_int32( MONGO_OP_INSERT, &h->op );
    write_le_int32( ( continue_on_err ) ? 1 : 0, ( p + sizeof(mongo_header)) );

    p+= sizeof(mongo_header) + 4;

    len = strlen(ns);
    memcpy( p, ns, len+1 );

    p+=len+1;

    return p;
}

#endif
