/* net.c */

/*    Copyright 2009-2011 10gen Inc.
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

/* Implementation for generic version of net.h */
#include "net.h"
#include <errno.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#ifndef NI_MAXSERV
# define NI_MAXSERV 32
#endif
#ifndef MSG_NOSIGNAL
# define MSG_NOSIGNAL 0
#endif

int mongo_write_socket( mongo *conn, const void *buf, size_t len ) {
    const char *cbuf = buf;
    int flags = MSG_NOSIGNAL;

    while ( len ) {
        size_t sent = send( conn->sock, cbuf, len, flags );
        if ( sent == -1 ) {
            if (errno == EPIPE) 
                conn->connected = 0;
            conn->err = MONGO_IO_ERROR;
            return MONGO_ERROR;
        }
        cbuf += sent;
        len -= sent;
    }

    return MONGO_OK;
}

int mongo_read_socket( mongo *conn, void *buf, size_t len ) {
    char *cbuf = buf;
    while ( len ) {
        size_t sent = recv( conn->sock, cbuf, len, 0 );
        if ( sent == 0 || sent == -1 ) {
            conn->err = MONGO_IO_ERROR;
            return MONGO_ERROR;
        }
        cbuf += sent;
        len -= sent;
    }

    return MONGO_OK;
}

/* This is a no-op in the generic implementation. */
int mongo_set_socket_op_timeout( mongo *conn, int millis ) {
    return MONGO_OK;
}

#ifdef _MONGO_USE_GETADDRINFO
int mongo_socket_connect( mongo *conn, const char *host, int port ) {
    char port_str[NI_MAXSERV];
    int status;

    struct addrinfo ai_hints;
    struct addrinfo *ai_list = NULL;
    struct addrinfo *ai_ptr = NULL;

    conn->sock = 0;
    conn->connected = 0;

    bson_sprintf( port_str, "%d", port );

    memset( &ai_hints, 0, sizeof( ai_hints ) );
#ifdef AI_ADDRCONFIG
    ai_hints.ai_flags = AI_ADDRCONFIG;
#endif
    ai_hints.ai_family = AF_UNSPEC;
    ai_hints.ai_socktype = SOCK_STREAM;

    status = getaddrinfo( host, port_str, &ai_hints, &ai_list );
    if ( status != 0 ) {
        bson_errprintf( "getaddrinfo failed: %s", gai_strerror( status ) );
        conn->err = MONGO_CONN_ADDR_FAIL;
        return MONGO_ERROR;
    }

    for ( ai_ptr = ai_list; ai_ptr != NULL; ai_ptr = ai_ptr->ai_next ) {
        conn->sock = socket( ai_ptr->ai_family, ai_ptr->ai_socktype, ai_ptr->ai_protocol );
        if ( conn->sock < 0 ) {
            conn->sock = 0;
            continue;
        }

        status = connect( conn->sock, ai_ptr->ai_addr, ai_ptr->ai_addrlen );
        if ( status != 0 ) {
            mongo_close_socket( conn->sock );
            conn->sock = 0;
            continue;
        }

        if ( ai_ptr->ai_protocol == IPPROTO_TCP ) {
            int flag = 1;

            setsockopt( conn->sock, IPPROTO_TCP, TCP_NODELAY,
                        ( void * ) &flag, sizeof( flag ) );
            if ( conn->op_timeout_ms > 0 )
                mongo_set_socket_op_timeout( conn, conn->op_timeout_ms );
        }

        conn->connected = 1;
        break;
    }

    freeaddrinfo( ai_list );

    if ( ! conn->connected ) {
        conn->err = MONGO_CONN_FAIL;
        return MONGO_ERROR;
    }

    return MONGO_OK;
}

#else
int mongo_socket_connect( mongo *conn, const char *host, int port ) {
    struct addrinfo req, *ans = NULL, *ans_cursor;
    int flag = 1;

    if ( ( conn->sock = socket( AF_INET, SOCK_STREAM, 0 ) ) < 0 ) {
        conn->sock = 0;
        conn->err = MONGO_CONN_NO_SOCKET;
        return MONGO_ERROR;
    }

    freeaddrinfo( ans );
    if ( ans_cursor ) {
        setsockopt( conn->sock, IPPROTO_TCP, TCP_NODELAY, ( char * ) &flag, sizeof( flag ) );
        if ( conn->op_timeout_ms > 0 )
            mongo_set_socket_op_timeout( conn, conn->op_timeout_ms );

        conn->connected = 1;
        return MONGO_OK;
    } else {
        conn->connected = 0;
        conn->sock = 0;
        conn->err = MONGO_CONN_FAIL;
        return MONGO_ERROR;
    }

    setsockopt( conn->sock, IPPROTO_TCP, TCP_NODELAY, ( char * ) &flag, sizeof( flag ) );

    if( conn->op_timeout_ms > 0 )
        mongo_set_socket_op_timeout( conn, conn->op_timeout_ms );

    conn->connected = 1;

    return MONGO_OK;
}

#endif

int mongo_sock_init( void ) {

#if defined(_WIN32)
    WSADATA wsaData;
    WORD wVers;
#elif defined(SIGPIPE)
    struct sigaction act;
#endif

    static int called_once;
    static int retval;
    if (called_once) return retval;
    called_once = 1;

#if defined(_WIN32)
    wVers = MAKEWORD(1, 1);
    retval = (WSAStartup(wVers, &wsaData) == 0);
#elif defined(MACINTOSH)
    GUSISetup(GUSIwithInternetSockets);
    retval = 1;
#elif defined(SIGPIPE)
    retval = 1;
    if (sigaction(SIGPIPE, (struct sigaction *)NULL, &act) < 0)
        retval = 0;
    else if (act.sa_handler == SIG_DFL) {
        act.sa_handler = SIG_IGN;
        if (sigaction(SIGPIPE, &act, (struct sigaction *)NULL) < 0)
            retval = 0;
    }
#endif
    return retval;
}
