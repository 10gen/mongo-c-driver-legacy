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
#include <string.h>
#include <errno.h>

#define READ 1
#define WRITE 2
#define CONNECT 3

#define false 0
#define true 1

void sig_pipe_handler(int signum) {
    return;
}

int wait_for_socket( mongo *conn, int type ) {
    struct timeval tv;
    int seconds, microseconds;
    if ( type == CONNECT ) {
        seconds = conn->conn_timeout_ms / 1000;
        microseconds = ( conn->conn_timeout_ms % 1000 ) * 1000;
    } else {
        seconds = conn->op_timeout_ms / 1000;
        microseconds = ( conn->op_timeout_ms % 1000 ) * 1000;
    }
    tv.tv_sec = seconds;
    tv.tv_usec = microseconds;

    int n, len;
    fd_set wset, rset;
    FD_ZERO( &wset );
    FD_ZERO( &rset );
    FD_SET( conn->sock, &rset );
    wset = rset;

    int error = 0;

    if ( select( conn->sock + 1, &rset, &wset, NULL, &tv ) == 0 ) {
        return false;
    }

    if ( type == CONNECT ) {
        if ( FD_ISSET( conn->sock, &wset ) || FD_ISSET( conn->sock, &rset ) ) {
            len = sizeof( error );
            if ( getsockopt( conn->sock, SOL_SOCKET, SO_ERROR, &error, &len ) < 0 ) {
                return false;
            }
        } else {
            return false;
        }
    }
    if ( error ) {
        return false;
    }
    return true;
}

int mongo_write_socket( mongo *conn, const void *buf, int len ) {
    signal(SIGINT, sig_pipe_handler);
    signal(SIGINT, SIG_IGN);

    struct timeval tv;
    int seconds, microseconds;
    seconds = conn->conn_timeout_ms / 1000;
    microseconds = ( conn->op_timeout_ms % 1000 ) * 1000;
    tv.tv_sec = seconds;
    tv.tv_usec = microseconds;

    const char *cbuf = buf;

    while ( len ) {
        if ( !wait_for_socket( conn, WRITE ) ) {
            conn->err = MONGO_READ_TIMEOUT;
            return MONGO_ERROR;
        }
        int sent = send( conn->sock, cbuf, len, 0 );
        if ( sent == -1 ) {
            conn->err = MONGO_IO_ERROR;
            return MONGO_ERROR;
        }
        cbuf += sent;
        len -= sent;
    }
    return MONGO_OK;
}

int mongo_read_socket( mongo *conn, void *buf, int len ) {
    struct timeval tv;
    int seconds, microseconds;
    seconds = conn->conn_timeout_ms / 1000;
    microseconds = ( conn->op_timeout_ms % 1000 ) * 1000;
    tv.tv_sec = seconds;
    tv.tv_usec = microseconds;

    char *cbuf = buf;
    
    while ( len ) {
        if ( !wait_for_socket( conn, READ ) ) {
            conn->err = MONGO_READ_TIMEOUT;
            return MONGO_ERROR;
        }   
        int sent = recv( conn->sock, cbuf, len, 0 );
        if ( sent == 0 || sent == -1 ) {
            conn->err = MONGO_IO_ERROR;
            return MONGO_ERROR;
        }
        cbuf += sent;
        len -= sent;
    }
    return MONGO_OK;
}

static int mongo_create_socket( mongo *conn ) {
    int fd;

    if( ( fd = socket( AF_INET, SOCK_STREAM, 0 ) ) == -1 ) {
        conn->err = MONGO_CONN_NO_SOCKET;
        return MONGO_ERROR;
    }

    conn->sock = fd;

    return MONGO_OK;
}

int mongo_cleanup_connection( mongo *conn, int error_type ) {
    mongo_close_socket( conn->sock );
    conn->connected = 0;
    conn->sock = 0;
    conn->err = error_type;
    return MONGO_ERROR;
}

int mongo_socket_connect( mongo *conn, const char *host, int port ) {
    struct sockaddr_in sa;
    int flags, n, error;
    socklen_t len, addressSize;
    fd_set rset, wset;
    struct timeval tv;

    tv.tv_sec = 0;
    tv.tv_usec = conn->conn_timeout_ms * 1000;

    int flag = 1;

    if( mongo_create_socket( conn ) != MONGO_OK )
        return MONGO_ERROR;

    flags = fcntl( conn->sock, F_GETFL, 0 );
    fcntl( conn->sock, F_SETFL, flags | O_NONBLOCK);

    memset( sa.sin_zero , 0 , sizeof( sa.sin_zero ) );
    sa.sin_family = AF_INET;
    sa.sin_port = htons( port );
    sa.sin_addr.s_addr = inet_addr( host );
    addressSize = sizeof( sa );

    if (( n = connect( conn->sock, ( struct sockaddr *)&sa, addressSize ) ) < 0 ) {
        if ( errno != EINPROGRESS ) {
            return mongo_cleanup_connection( conn, MONGO_CONN_FAIL );
        }
    }

    if ( n == 0) {
        goto done;
    }

    if ( !wait_for_socket( conn, CONNECT ) ) {
        return mongo_cleanup_connection( conn, MONGO_CONN_TIMEOUT );
    }

done:
    fcntl( conn->sock, F_SETFL, flags);
    setsockopt( conn->sock, IPPROTO_TCP, TCP_NODELAY, ( char * ) &flag, sizeof( flag ) );
    conn->connected = 1;
    return MONGO_OK;
}
