/* mongo.c */

/*    Copyright 2009, 2010 10gen Inc.
 *    Copyright 2010 Kaspersky Labs GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "mongo.h"
#include "md5.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#ifndef _WIN32
#include <unistd.h>
#endif

#if defined(MONGO_ASYNC)
#include <poll.h>
#include <errno.h>
#endif

/* only need one of these */
static const int zero = 0;
static const int one = 1;

#if defined(MONGO_ASYNC)

static inline void mongo_buffer_append(mongo_connection_buffer * buffer, void * data, size_t length)
{
    if(!buffer->data)
    {
        buffer->data = malloc(512);
        buffer->size = 512;
        buffer->length = 0;
        buffer->offset = 0;
    }

    if(buffer->offset + buffer->length + length >= buffer->size)
    { /* We would write over the end of allocated space... */
        if(buffer->length + length >= buffer->size)
        { /* Not enough space in current buffer, allocate more. */
        #if defined(__x86_64__) || defined(__x86__)
            buffer->size = buffer->length + length;
            __asm__("bsr %0, %%ecx\n\tmov $2, %0\n\tshl %%cl, %0" : "=g" (buffer->size) : "0" (buffer->size) : "ecx" );
        #else
            do {
                buffer->size = buffer->size << 1;
            } while(buffer->length + length >= buffer->size);
        #endif

            buffer->data = realloc(buffer->data, buffer->size);

            if(buffer->offset + buffer->length + length < buffer->size) {
                memmove(buffer->data, buffer->data + buffer->offset, buffer->length);
                buffer->offset = 0;
            }
        } else /* invariant: (buffer->offset + (buffer->size - buffer->length) >= length) */
        { /* We have enough space but we need to move something.. FIXME: benchmark maximum move size */
            memmove(buffer->data, buffer->data + buffer->offset, buffer->length);
            buffer->offset = 0;
        }
    }

    memcpy(buffer->data + buffer->offset + buffer->length, data, length);
    buffer->length += length;
}

static inline void mongo_buffer_erase(mongo_connection_buffer * buffer, size_t erased)
{
    if(erased > buffer->length)
        erased = buffer->length;

    buffer->offset += erased;
    buffer->length -= erased;

    if(buffer->length < buffer->size >> 1 && buffer->size > 512) {
    #if defined(__x86_64__) || defined(__x86__)
        buffer->size = buffer->length;
        __asm__("bsr %0, %%ecx\n\tmov $2, %0\n\tshl %%cl, %0" : "=g" (buffer->size) : "0" (buffer->size) : "ecx" );
    #else
        do {
            buffer->size = buffer->size >> 1;
        } while(buffer->length >= buffer->size);
    #endif

        memmove(buffer->data, buffer->data + buffer->offset, buffer->length);
        buffer->offset = 0;
        buffer->data = realloc(buffer->data, buffer->size);
    }
}


#endif /* MONGO_ASYNC */

/* ----------------------------
   message stuff
   ------------------------------ */

static void looping_write(mongo_connection * conn, const void* buf, int len){
    const char* cbuf = buf;
    while (len){
        int sent = send(conn->sock, cbuf, len, 0);
        if (sent == -1) MONGO_THROW(MONGO_EXCEPT_NETWORK);
        cbuf += sent;
        len -= sent;
    }
}

static void looping_read(mongo_connection * conn, void* buf, int len){
    char* cbuf = buf;
    while (len){
        int sent = recv(conn->sock, cbuf, len, 0);
        if (sent == 0 || sent == -1) MONGO_THROW(MONGO_EXCEPT_NETWORK);
        cbuf += sent;
        len -= sent;
    }
}

/* Always calls free(mm) */
void mongo_message_send(mongo_connection * conn, mongo_message* mm){
    mongo_header head; /* little endian */
    bson_little_endian32(&head.len, &mm->head.len);
    bson_little_endian32(&head.id, &mm->head.id);
    bson_little_endian32(&head.responseTo, &mm->head.responseTo);
    bson_little_endian32(&head.op, &mm->head.op);

#if defined(MONGO_ASYNC)
    if(conn->async){        
        mongo_buffer_append(&conn->out, &head, sizeof(head));
        mongo_buffer_append(&conn->out, &mm->data, mm->head.len - sizeof(head));
    } else
#endif
    {
        MONGO_TRY{
            looping_write(conn, &head, sizeof(head));
            looping_write(conn, &mm->data, mm->head.len - sizeof(head));
        }MONGO_CATCH{
            free(mm);
            MONGO_RETHROW();
        }
    }

    free(mm);
}

char * mongo_data_append( char * start , const void * data , int len ){
    memcpy( start , data , len );
    return start + len;
}

char * mongo_data_append32( char * start , const void * data){
    bson_little_endian32( start , data );
    return start + 4;
}

char * mongo_data_append64( char * start , const void * data){
    bson_little_endian64( start , data );
    return start + 8;
}

mongo_message * mongo_message_create( int len , int id , int responseTo , int op ){
    mongo_message * mm = (mongo_message*)bson_malloc( len );

    if (!id)
        id = rand();

    /* native endian (converted on send) */
    mm->head.len = len;
    mm->head.id = id;
    mm->head.responseTo = responseTo;
    mm->head.op = op;

    return mm;
}

/* ----------------------------
   connection stuff
   ------------------------------ */
static int mongo_connect_helper( mongo_connection * conn ){
    /* setup */
    conn->sock = 0;
    conn->connected = 0;

    memset( conn->sa.sin_zero , 0 , sizeof(conn->sa.sin_zero) );
    conn->sa.sin_family = AF_INET;
    conn->sa.sin_port = htons(conn->left_opts->port);
    conn->sa.sin_addr.s_addr = inet_addr( conn->left_opts->host );
    conn->addressSize = sizeof(conn->sa);

    /* connect */
    conn->sock = socket( AF_INET, SOCK_STREAM, 0 );
    if ( conn->sock <= 0 ){
        return mongo_conn_no_socket;
    }

    if ( connect( conn->sock , (struct sockaddr*)&conn->sa , conn->addressSize ) ){
        return mongo_conn_fail;
    }

    /* nagle */
    setsockopt( conn->sock, IPPROTO_TCP, TCP_NODELAY, (char *) &one, sizeof(one) );

    /* TODO signals */

    conn->connected = 1;
    return 0;
}

mongo_conn_return mongo_connect( mongo_connection * conn , mongo_connection_options * options ){
    MONGO_INIT_EXCEPTION(&conn->exception);

#if defined(MONGO_ASYNC)
    conn->async = 0;
#endif

    conn->left_opts = bson_malloc(sizeof(mongo_connection_options));
    conn->right_opts = NULL;

    if ( options ){
        memcpy( conn->left_opts , options , sizeof( mongo_connection_options ) );
    } else {
        strcpy( conn->left_opts->host , "127.0.0.1" );
        conn->left_opts->port = 27017;
    }

    return mongo_connect_helper(conn);
}

static void swap_repl_pair(mongo_connection * conn){
    mongo_connection_options * tmp = conn->left_opts;
    conn->left_opts = conn->right_opts;
    conn->right_opts = tmp;
}

mongo_conn_return mongo_connect_pair( mongo_connection * conn , mongo_connection_options * left, mongo_connection_options * right ){
    conn->connected = 0;
    MONGO_INIT_EXCEPTION(&conn->exception);

#if defined(MONGO_ASYNC)
    conn->async = 0;
#endif

    conn->left_opts = NULL;
    conn->right_opts = NULL;

    if ( !left || !right )
        return mongo_conn_bad_arg;

    conn->left_opts = bson_malloc(sizeof(mongo_connection_options));
    conn->right_opts = bson_malloc(sizeof(mongo_connection_options));

    memcpy( conn->left_opts,  left,  sizeof( mongo_connection_options ) );
    memcpy( conn->right_opts, right, sizeof( mongo_connection_options ) );
    
    return mongo_reconnect(conn);
}

mongo_conn_return mongo_reconnect( mongo_connection * conn ){
    mongo_conn_return ret;
    mongo_disconnect(conn);

    /* single server */
    if(conn->right_opts == NULL)
        return mongo_connect_helper(conn);

    /* repl pair */
    ret = mongo_connect_helper(conn);
    if (ret == mongo_conn_success && mongo_cmd_ismaster(conn, NULL)){
        return mongo_conn_success;
    }

    swap_repl_pair(conn);

    ret = mongo_connect_helper(conn);
    if (ret == mongo_conn_success){
        if(mongo_cmd_ismaster(conn, NULL))
            return mongo_conn_success;
        else
            return mongo_conn_not_master;
    }

    /* failed to connect to both servers */
    return ret;
}


#if defined(MONGO_ASYNC)

void mongo_async_create( mongo_connection * conn, int fd )
{
    conn->left_opts = conn->right_opts = NULL;
    conn->sock = fd;
    conn->connected = 1;
    conn->async = 1;

    conn->in.data = 0;
    conn->in.length = 0;
    conn->out.data = 0;
    conn->out.length = 0;
}

short mongo_async_pollmask( mongo_connection * conn )
{
    short result = POLLERR | POLLHUP | POLLIN;

    if(conn->out.length)
        result |= POLLOUT;

    return result;
}

int mongo_async_consume( mongo_connection * conn, short events )
{
    uint8_t buffer[4096];
    int result;

    if(events & (POLLERR | POLLHUP))
        return -1;

    if(events & POLLOUT)
    {
        result = send(conn->sock, conn->out.data + conn->out.offset, conn->out.length, 0);
        
        if(result > 0)
            mongo_buffer_erase(&conn->out, result);
        else if(result == 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
            return -1;
    }

    if(events & POLLIN)
    {
        uint32_t length;
        result = recv(conn->sock, buffer, sizeof(buffer), 0);

        if(result > 0)
            mongo_buffer_append(&conn->in, buffer, result);
        else if(result == 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
            return -1;

        if(conn->in.length < sizeof(uint32_t))
            return 0;

        bson_little_endian32(&length, (uint32_t *) &conn->in.data[conn->in.offset]);
        return conn->in.length >= length;
    }

    return 0;
}

#endif /* MONGO_ASYNC */


void mongo_insert_batch( mongo_connection * conn , const char * ns , bson ** bsons, int count){
    int size =  16 + 4 + strlen( ns ) + 1;
    int i;
    mongo_message * mm;
    char* data;

    for(i=0; i<count; i++){
        size += bson_size(bsons[i]);
    }

    mm = mongo_message_create( size , 0 , 0 , mongo_op_insert );

    data = &mm->data;
    data = mongo_data_append32(data, &zero);
    data = mongo_data_append(data, ns, strlen(ns) + 1);

    for(i=0; i<count; i++){
        data = mongo_data_append(data, bsons[i]->data, bson_size( bsons[i] ) );
    }

    mongo_message_send(conn, mm);
}

void mongo_insert( mongo_connection * conn , const char * ns , bson * bson ){
    char * data;
    mongo_message * mm = mongo_message_create( 16 /* header */
                                             + 4 /* ZERO */
                                             + strlen(ns)
                                             + 1 + bson_size(bson)
                                             , 0, 0, mongo_op_insert);

    data = &mm->data;
    data = mongo_data_append32(data, &zero);
    data = mongo_data_append(data, ns, strlen(ns) + 1);
    data = mongo_data_append(data, bson->data, bson_size(bson));

    mongo_message_send(conn, mm);
}

void mongo_update(mongo_connection* conn, const char* ns, const bson* cond, const bson* op, int flags){
    char * data;
    mongo_message * mm = mongo_message_create( 16 /* header */
                                             + 4  /* ZERO */
                                             + strlen(ns) + 1
                                             + 4  /* flags */
                                             + bson_size(cond)
                                             + bson_size(op)
                                             , 0 , 0 , mongo_op_update );

    data = &mm->data;
    data = mongo_data_append32(data, &zero);
    data = mongo_data_append(data, ns, strlen(ns) + 1);
    data = mongo_data_append32(data, &flags);
    data = mongo_data_append(data, cond->data, bson_size(cond));
    data = mongo_data_append(data, op->data, bson_size(op));

    mongo_message_send(conn, mm);
}

void mongo_remove(mongo_connection* conn, const char* ns, const bson* cond){
    char * data;
    mongo_message * mm = mongo_message_create( 16 /* header */
                                             + 4  /* ZERO */
                                             + strlen(ns) + 1
                                             + 4  /* ZERO */
                                             + bson_size(cond)
                                             , 0 , 0 , mongo_op_delete );

    data = &mm->data;
    data = mongo_data_append32(data, &zero);
    data = mongo_data_append(data, ns, strlen(ns) + 1);
    data = mongo_data_append32(data, &zero);
    data = mongo_data_append(data, cond->data, bson_size(cond));

    mongo_message_send(conn, mm);
}

mongo_reply * mongo_read_response( mongo_connection * conn ){
    mongo_header head; /* header from network */
    mongo_reply_fields fields; /* header from network */
    mongo_reply * out; /* native endian */
    int len;

#if defined(MONGO_ASYNC)
    if(conn->async) {
        if(conn->in.length < sizeof(head) + sizeof(fields)) {
            MONGO_THROW(MONGO_EXCEPT_NETWORK);
        }

        memcpy(&head, &conn->in.data[conn->in.offset], sizeof(head));
        memcpy(&fields, &conn->in.data[conn->in.offset + sizeof(head)], sizeof(fields));
    } else
#endif
    {
        looping_read(conn, &head, sizeof(head));
        looping_read(conn, &fields, sizeof(fields));
    }

    bson_little_endian32(&len, &head.len);

    out = (mongo_reply*)bson_malloc(len);

    out->head.len = len;
    bson_little_endian32(&out->head.id, &head.id);
    bson_little_endian32(&out->head.responseTo, &head.responseTo);
    bson_little_endian32(&out->head.op, &head.op);

    bson_little_endian32(&out->fields.flag, &fields.flag);
    bson_little_endian64(&out->fields.cursorID, &fields.cursorID);
    bson_little_endian32(&out->fields.start, &fields.start);
    bson_little_endian32(&out->fields.num, &fields.num);

#if defined(MONGO_ASYNC)
    if(conn->async) {
        if(conn->in.length < len || len < sizeof(head) + sizeof(fields)) {
            MONGO_THROW(MONGO_EXCEPT_NETWORK);
        }

        memcpy(&out->objs, &conn->in.data[conn->in.offset + sizeof(head) + sizeof(fields)], len-sizeof(head)-sizeof(fields));
        mongo_buffer_erase(&conn->in, len);
    } else
#endif
    {
        if (len < sizeof(head)+sizeof(fields) || len > 64*1024*1024)
            MONGO_THROW(MONGO_EXCEPT_NETWORK); /* most likely corruption */

        MONGO_TRY{
            looping_read(conn, &out->objs, len-sizeof(head)-sizeof(fields));
        }MONGO_CATCH{
            free(out);
            MONGO_RETHROW();
        }
    }

    return out;
}


#if defined(MONGO_ASYNC)
mongo_cursor* mongo_find(mongo_connection* conn, const char* ns, bson* query, bson* fields, int nToReturn, int nToSkip, int options){
    mongo_find_request(conn, ns, query, fields, nToReturn, nToSkip, options);
	return mongo_find_response(conn, ns);
}

void mongo_find_request(mongo_connection* conn, const char* ns, bson* query, bson* fields, int nToReturn, int nToSkip, int options){
#else
mongo_cursor* mongo_find(mongo_connection* conn, const char* ns, bson* query, bson* fields, int nToReturn, int nToSkip, int options){
    int sl;
    mongo_cursor * cursor;
#endif
    char * data;
    mongo_message * mm = mongo_message_create( 16 + /* header */
                                               4 + /*  options */
                                               strlen( ns ) + 1 + /* ns */
                                               4 + 4 + /* skip,return */
                                               bson_size( query ) +
                                               bson_size( fields ) ,
                                               0 , 0 , mongo_op_query );


    data = &mm->data;
    data = mongo_data_append32( data , &options );
    data = mongo_data_append( data , ns , strlen( ns ) + 1 );    
    data = mongo_data_append32( data , &nToSkip );
    data = mongo_data_append32( data , &nToReturn );
    data = mongo_data_append( data , query->data , bson_size( query ) );    
    if ( fields )
        data = mongo_data_append( data , fields->data , bson_size( fields ) );    
    
    bson_fatal_msg( (data == ((char*)mm) + mm->head.len), "query building fail!" );

    mongo_message_send( conn , mm );
#if defined(MONGO_ASYNC)
}

mongo_cursor* mongo_find_response(mongo_connection* conn, const char* ns){
    int sl;
    mongo_cursor * cursor;
#endif

    cursor = (mongo_cursor*)bson_malloc(sizeof(mongo_cursor));

    MONGO_TRY{
        cursor->mm = mongo_read_response(conn);
    }MONGO_CATCH{
        free(cursor);
        MONGO_RETHROW();
    }

    sl = strlen(ns)+1;
    cursor->ns = bson_malloc(sl);
    if (!cursor->ns){
        free(cursor->mm);
        free(cursor);
        return 0;
    }
    memcpy((void*)cursor->ns, ns, sl); /* cast needed to silence GCC warning */
    cursor->conn = conn;
    cursor->current.data = NULL;
    return cursor;
}

bson_bool_t mongo_find_one(mongo_connection* conn, const char* ns, bson* query, bson* fields, bson* out){
    mongo_cursor* cursor = mongo_find(conn, ns, query, fields, 1, 0, 0);

    if (cursor && mongo_cursor_next(cursor)){
        bson_copy(out, &cursor->current);
        mongo_cursor_destroy(cursor);
        return 1;
    }else{
        mongo_cursor_destroy(cursor);
        return 0;
    }
}

int64_t mongo_count(mongo_connection* conn, const char* db, const char* ns, bson* query){
    bson_buffer bb;
    bson cmd;
    bson out;
    int64_t count = -1;

    bson_buffer_init(&bb);
    bson_append_string(&bb, "count", ns);
    if (query && bson_size(query) > 5) /* not empty */
        bson_append_bson(&bb, "query", query);
    bson_from_buffer(&cmd, &bb);

    MONGO_TRY{
        if(mongo_run_command(conn, db, &cmd, &out)){
            bson_iterator it;
            if(bson_find(&it, &out, "n"))
                count = bson_iterator_long(&it);
        }
    }MONGO_CATCH{
        bson_destroy(&cmd);
        MONGO_RETHROW();
    }
    
    bson_destroy(&cmd);
    bson_destroy(&out);
    return count;
}

bson_bool_t mongo_disconnect( mongo_connection * conn ){
#if defined(MONGO_ASYNC)
    if( conn->async ){
        return 0;
    }
#endif

    if ( ! conn->connected )
        return 1;

#ifdef _WIN32
    closesocket( conn->sock );
#else
    close( conn->sock );
#endif
    
    conn->sock = 0;
    conn->connected = 0;
    
    return 0;
}

bson_bool_t mongo_destroy( mongo_connection * conn ){
#if defined(MONGO_ASYNC)
    if(conn->async){
        if(conn->in.data){
            free(conn->in.data);
            conn->out.data = 0;
        }

        if(conn->out.data){
            free(conn->out.data);
            conn->out.data = 0;
        }
    }
#endif

    free(conn->left_opts);
    free(conn->right_opts);
    conn->left_opts = NULL;
    conn->right_opts = NULL;

    return mongo_disconnect( conn );
}

bson_bool_t mongo_cursor_get_more(mongo_cursor* cursor){
    if (cursor->mm && cursor->mm->fields.cursorID){
        mongo_connection* conn = cursor->conn;
        char* data;
        int sl = strlen(cursor->ns)+1;
        mongo_message * mm = mongo_message_create(16 /*header*/
                                                 +4 /*ZERO*/
                                                 +sl
                                                 +4 /*numToReturn*/
                                                 +8 /*cursorID*/
                                                 , 0, 0, mongo_op_get_more);
        data = &mm->data;
        data = mongo_data_append32(data, &zero);
        data = mongo_data_append(data, cursor->ns, sl);
        data = mongo_data_append32(data, &zero);
        data = mongo_data_append64(data, &cursor->mm->fields.cursorID);
        mongo_message_send(conn, mm);

        free(cursor->mm);

        MONGO_TRY{
            cursor->mm = mongo_read_response(cursor->conn);
        }MONGO_CATCH{
            cursor->mm = NULL;
#if defined(MONGO_ASYNC)
            if(!cursor->conn->async)
#endif
            {
                mongo_cursor_destroy(cursor);
            }
            MONGO_RETHROW();
        }

        return cursor->mm && cursor->mm->fields.num;
    } else{
        return 0;
    }
}

bson_bool_t mongo_cursor_next(mongo_cursor* cursor){
    char* bson_addr;

    /* no data */
    if (!cursor->mm || cursor->mm->fields.num == 0)
        return 0;

    /* first */
    if (cursor->current.data == NULL){
        bson_init(&cursor->current, &cursor->mm->objs, 0);
        return 1;
    }

    bson_addr = cursor->current.data + bson_size(&cursor->current);
    if (bson_addr >= ((char*)cursor->mm + cursor->mm->head.len)){
        if (!mongo_cursor_get_more(cursor))
            return 0;
        bson_init(&cursor->current, &cursor->mm->objs, 0);
    } else {
        bson_init(&cursor->current, bson_addr, 0);
    }

    return 1;
}

void mongo_cursor_destroy(mongo_cursor* cursor){
    if (!cursor) return;

    if (cursor->mm && cursor->mm->fields.cursorID){
        mongo_connection* conn = cursor->conn;
        mongo_message * mm = mongo_message_create(16 /*header*/
                                                 +4 /*ZERO*/
                                                 +4 /*numCursors*/
                                                 +8 /*cursorID*/
                                                 , 0, 0, mongo_op_kill_cursors);
        char* data = &mm->data;
        data = mongo_data_append32(data, &zero);
        data = mongo_data_append32(data, &one);
        data = mongo_data_append64(data, &cursor->mm->fields.cursorID);
        
        MONGO_TRY{
            mongo_message_send(conn, mm);
        }MONGO_CATCH{
            free(cursor->mm);
            free((void*)cursor->ns);
            free(cursor);
            MONGO_RETHROW();
        }
    }
        
    free(cursor->mm);
    free((void*)cursor->ns);
    free(cursor);
}

bson_bool_t mongo_create_index(mongo_connection * conn, const char * ns, bson * key, int options, bson * out){
    bson_buffer bb;
    bson b;
    bson_iterator it;
    char name[255] = {'_'};
    int i = 1;
    char idxns[1024];

    bson_iterator_init(&it, key->data);
    while(i < 255 && bson_iterator_next(&it)){
        strncpy(name + i, bson_iterator_key(&it), 255 - i);
        i += strlen(bson_iterator_key(&it));
    }
    name[254] = '\0';

    bson_buffer_init(&bb);
    bson_append_bson(&bb, "key", key);
    bson_append_string(&bb, "ns", ns);
    bson_append_string(&bb, "name", name);
    if (options & MONGO_INDEX_UNIQUE)
        bson_append_bool(&bb, "unique", 1);
    if (options & MONGO_INDEX_DROP_DUPS)
        bson_append_bool(&bb, "dropDups", 1);
    
    bson_from_buffer(&b, &bb);

    strncpy(idxns, ns, 1024-16);
    strcpy(strchr(idxns, '.'), ".system.indexes");
    mongo_insert(conn, idxns, &b);
    bson_destroy(&b);

    *strchr(idxns, '.') = '\0'; /* just db not ns */
    return !mongo_cmd_get_last_error(conn, idxns, out);
}
bson_bool_t mongo_create_simple_index(mongo_connection * conn, const char * ns, const char* field, int options, bson * out){
    bson_buffer bb;
    bson b;
    bson_bool_t success;

    bson_buffer_init(&bb);
    bson_append_int(&bb, field, 1);
    bson_from_buffer(&b, &bb);

    success = mongo_create_index(conn, ns, &b, options, out);
    bson_destroy(&b);
    return success;
}

bson_bool_t mongo_run_command(mongo_connection * conn, const char * db, bson * command, bson * out){
    bson fields;
    int sl = strlen(db);
    char* ns = bson_malloc(sl + 5 + 1); /* ".$cmd" + nul */
    bson_bool_t success;

    strcpy(ns, db);
    strcpy(ns+sl, ".$cmd");

    success = mongo_find_one(conn, ns, command, bson_empty(&fields), out);
    free(ns);
    return success;
}
bson_bool_t mongo_simple_int_command(mongo_connection * conn, const char * db, const char* cmdstr, int arg, bson * realout){
    bson out;
    bson cmd;
    bson_buffer bb;
    bson_bool_t success = 0;

    bson_buffer_init(&bb);
    bson_append_int(&bb, cmdstr, arg);
    bson_from_buffer(&cmd, &bb);

    if(mongo_run_command(conn, db, &cmd, &out)){
        bson_iterator it;
        if(bson_find(&it, &out, "ok"))
            success = bson_iterator_bool(&it);
    }
    
    bson_destroy(&cmd);

    if (realout)
        *realout = out;
    else
        bson_destroy(&out);

    return success;
}

bson_bool_t mongo_simple_str_command(mongo_connection * conn, const char * db, const char* cmdstr, const char* arg, bson * realout){
    bson out;
    bson cmd;
    bson_buffer bb;
    bson_bool_t success = 0;

    bson_buffer_init(&bb);
    bson_append_string(&bb, cmdstr, arg);
    bson_from_buffer(&cmd, &bb);

    if(mongo_run_command(conn, db, &cmd, &out)){
        bson_iterator it;
        if(bson_find(&it, &out, "ok"))
            success = bson_iterator_bool(&it);
    }
    
    bson_destroy(&cmd);

    if (realout)
        *realout = out;
    else
        bson_destroy(&out);

    return success;
}

bson_bool_t mongo_cmd_drop_db(mongo_connection * conn, const char * db){
    return mongo_simple_int_command(conn, db, "dropDatabase", 1, NULL);
}

bson_bool_t mongo_cmd_drop_collection(mongo_connection * conn, const char * db, const char * collection, bson * out){
    return mongo_simple_str_command(conn, db, "drop", collection, out);
}

void mongo_cmd_reset_error(mongo_connection * conn, const char * db){
    mongo_simple_int_command(conn, db, "reseterror", 1, NULL);
}

static bson_bool_t mongo_cmd_get_error_helper(mongo_connection * conn, const char * db, bson * realout, const char * cmdtype){
    bson out = {NULL,0};
    bson_bool_t haserror = 1;


    if(mongo_simple_int_command(conn, db, cmdtype, 1, &out)){
        bson_iterator it;
        haserror = (bson_find(&it, &out, "err") != bson_null);
    }
    
    if(realout)
        *realout = out; /* transfer of ownership */
    else
        bson_destroy(&out);

    return haserror;
}

bson_bool_t mongo_cmd_get_prev_error(mongo_connection * conn, const char * db, bson * out){
    return mongo_cmd_get_error_helper(conn, db, out, "getpreverror");
}
bson_bool_t mongo_cmd_get_last_error(mongo_connection * conn, const char * db, bson * out){
    return mongo_cmd_get_error_helper(conn, db, out, "getlasterror");
}

bson_bool_t mongo_cmd_ismaster(mongo_connection * conn, bson * realout){
    bson out = {NULL,0};
    bson_bool_t ismaster = 0;

    if (mongo_simple_int_command(conn, "admin", "ismaster", 1, &out)){
        bson_iterator it;
        bson_find(&it, &out, "ismaster");
        ismaster = bson_iterator_bool(&it);
    }

    if(realout)
        *realout = out; /* transfer of ownership */
    else
        bson_destroy(&out);

    return ismaster;
}

static void digest2hex(mongo_md5_byte_t digest[16], char hex_digest[33]){
    static const char hex[16] = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    int i;
    for (i=0; i<16; i++){
        hex_digest[2*i]     = hex[(digest[i] & 0xf0) >> 4];
        hex_digest[2*i + 1] = hex[ digest[i] & 0x0f      ];
    }
    hex_digest[32] = '\0';
}

static void mongo_pass_digest(const char* user, const char* pass, char hex_digest[33]){
    mongo_md5_state_t st;
    mongo_md5_byte_t digest[16];

    mongo_md5_init(&st);
    mongo_md5_append(&st, (const mongo_md5_byte_t*)user, strlen(user));
    mongo_md5_append(&st, (const mongo_md5_byte_t*)":mongo:", 7);
    mongo_md5_append(&st, (const mongo_md5_byte_t*)pass, strlen(pass));
    mongo_md5_finish(&st, digest);
    digest2hex(digest, hex_digest);
}

void mongo_cmd_add_user(mongo_connection* conn, const char* db, const char* user, const char* pass){
    bson_buffer bb;
    bson user_obj;
    bson pass_obj;
    char hex_digest[33];
    char* ns = malloc(strlen(db) + strlen(".system.users") + 1);

    strcpy(ns, db);
    strcpy(ns+strlen(db), ".system.users");

    mongo_pass_digest(user, pass, hex_digest);

    bson_buffer_init(&bb);
    bson_append_string(&bb, "user", user);
    bson_from_buffer(&user_obj, &bb);

    bson_buffer_init(&bb);
    bson_append_start_object(&bb, "$set");
    bson_append_string(&bb, "pwd", hex_digest);
    bson_append_finish_object(&bb);
    bson_from_buffer(&pass_obj, &bb);


    MONGO_TRY{
        mongo_update(conn, ns, &user_obj, &pass_obj, MONGO_UPDATE_UPSERT);
    }MONGO_CATCH{
        free(ns);
        bson_destroy(&user_obj);
        bson_destroy(&pass_obj);
        MONGO_RETHROW();
    }

    free(ns);
    bson_destroy(&user_obj);
    bson_destroy(&pass_obj);
}

bson_bool_t mongo_cmd_authenticate(mongo_connection* conn, const char* db, const char* user, const char* pass){
    bson_buffer bb;
    bson from_db, auth_cmd;
    const char* nonce;
    bson_bool_t success = 0;

    mongo_md5_state_t st;
    mongo_md5_byte_t digest[16];
    char hex_digest[33];

    if (mongo_simple_int_command(conn, db, "getnonce", 1, &from_db)){
        bson_iterator it;
        bson_find(&it, &from_db, "nonce");
        nonce = bson_iterator_string(&it);
    }else{
        return 0;
    }

    mongo_pass_digest(user, pass, hex_digest);

    mongo_md5_init(&st);
    mongo_md5_append(&st, (const mongo_md5_byte_t*)nonce, strlen(nonce));
    mongo_md5_append(&st, (const mongo_md5_byte_t*)user, strlen(user));
    mongo_md5_append(&st, (const mongo_md5_byte_t*)hex_digest, 32);
    mongo_md5_finish(&st, digest);
    digest2hex(digest, hex_digest);

    bson_buffer_init(&bb);
    bson_append_int(&bb, "authenticate", 1);
    bson_append_string(&bb, "user", user);
    bson_append_string(&bb, "nonce", nonce);
    bson_append_string(&bb, "key", hex_digest);
    bson_from_buffer(&auth_cmd, &bb);

    bson_destroy(&from_db);

    MONGO_TRY{
        if(mongo_run_command(conn, db, &auth_cmd, &from_db)){
            bson_iterator it;
            if(bson_find(&it, &from_db, "ok"))
                success = bson_iterator_bool(&it);
        }
    }MONGO_CATCH{
        bson_destroy(&auth_cmd);
        MONGO_RETHROW();
    }

    bson_destroy(&from_db);
    bson_destroy(&auth_cmd);

    return success;
}
