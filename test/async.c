/* async.c */

#include "test.h"
#include "mongo.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <poll.h>
#include <stdint.h>
#include <errno.h>


#define ASSURE(cmd) \
    if(!(cmd)) { \
        perror("Error -- " #cmd); \
        return -1; \
    }


static const size_t SAMPLES = 4096;

enum
{
    RANDOM = 0,
    SOCKET,
    NFDS
};


static uint8_t random_buffer[16];
static size_t random_offset, fetches;

int main() {
    struct pollfd fds[2];
    int random_fd, socket_fd;
    struct sockaddr_in server_addr;
    mongo_connection mongo;
    mongo_cursor * cursor;
    size_t inserts, error_requests;

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(0x7f000001);
    server_addr.sin_port = htons(27017);

    ASSURE((fds[RANDOM].fd = open("/dev/urandom", O_RDONLY | O_NONBLOCK)) >= 0);
    ASSURE((fds[SOCKET].fd = socket(AF_INET, SOCK_STREAM, 0)) >= 0);
    /* TODO: if this is used as example code, add non-blocking connect */
    ASSURE(connect(fds[SOCKET].fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) >= 0);
    ASSURE(fcntl(fds[SOCKET].fd, F_SETFL, O_NONBLOCK) >= 0);

    mongo_async_create(&mongo, fds[SOCKET].fd);
    random_offset = 0;

    {
        bson doc;
        bson_buffer buffer;
        bson_buffer_init(&buffer);

        bson_append_string(&buffer, "drop", "random");
        bson_from_buffer(&doc, &buffer);
        mongo_find_request(&mongo, "test.$cmd", &doc, 0, 1, 0, 0);

        error_requests = 1;
    }

    for(inserts = 0; inserts < SAMPLES || error_requests;)
    {
        fds[RANDOM].events = (random_offset < sizeof(random_buffer) ? POLLIN | POLLERR : POLLERR);
        fds[RANDOM].revents = 0;

        fds[SOCKET].events = mongo_async_pollmask(&mongo);
        fds[SOCKET].revents = 0;

        if(inserts < 4096)
        {
            ASSURE(poll(fds, NFDS, -1) >= 0);
        }
        else
        {
            ASSURE(poll(&fds[SOCKET], 1, -1) >= 0);
        }

        ASSURE(!(fds[RANDOM].revents & POLLERR) && !(fds[SOCKET].revents & POLLERR));

        if(random_offset < sizeof(random_buffer) && fds[RANDOM].revents & POLLIN)
        {
            int res = read(fds[RANDOM].fd, &random_buffer[random_offset], sizeof(random_buffer) - random_offset);
            ASSURE(res >= 0 || errno == EAGAIN);
            random_offset += (size_t) res;
        }

        if(random_offset == sizeof(random_buffer))
        {
            bson doc;
            bson_buffer buffer;

            bson_buffer_init(&buffer);
            bson_append_binary(&buffer, "_id", bson_bindata, random_buffer, sizeof(random_buffer));
            bson_from_buffer(&doc, &buffer);
            mongo_insert(&mongo, "test.random", &doc);
            bson_destroy(&doc);

            random_offset = 0;

            ++inserts;
        }
        
        if(inserts == SAMPLES - 1)
        {
            bson doc;
            bson_buffer buffer;

            bson_buffer_init(&buffer);
            bson_append_int(&buffer, "getpreverror", 1);
            bson_from_buffer(&doc, &buffer);
            mongo_find_request(&mongo, "test.$cmd", &doc, 0, 1, 0, 0);

            ++error_requests;
        }

        if(fds[SOCKET].revents)
        {
            int res;
            
            ASSURE((res = mongo_async_consume(&mongo, fds[SOCKET].revents)) >= 0);

            if(res == 0)
                continue;

            MONGO_TRY_GENERIC(&mongo)
            {
                cursor = mongo_find_response(&mongo, "test.$cmd");
                bson_iterator it;

                if(!mongo_cursor_next(cursor))
                    return -1;

                if(!bson_find(&it, &cursor->current, "err"))
                {
                    if(bson_find(&it, &cursor->current, "errmsg"))
                    { /* This is the result of the drop command */
                        if(bson_iterator_type(&it) != bson_string)
                            return -1;

                        fprintf(stderr, "Drop failed: %s\n", bson_iterator_string(&it));
                    }
                    else
                        return -1;
                }
                else
                {
                    if(bson_iterator_type(&it) != bson_null)
                    {
                        fprintf(stderr, "Error inserting: %s\n", bson_iterator_string(&it));
                        return -1;
                    }
                }

                mongo_cursor_destroy(cursor);
                --error_requests;
            }
            MONGO_CATCH_GENERIC(&mongo)
            {
                return -1;
            }
        }
    }

    {
        bson doc;

        mongo_find_request(&mongo, "test.random", bson_empty(&doc), 0, 0, 0, 0);
    }

    for(fetches = 0, cursor = 0;;)
    {
        fds[SOCKET].events = mongo_async_pollmask(&mongo);
        fds[SOCKET].revents = 0;
        
        ASSURE(poll(&fds[SOCKET], 1, -1) >= 0);
        ASSURE(!(fds[SOCKET].revents & POLLERR));
        
        if(fds[SOCKET].revents)
        {
            int res;
            
            ASSURE((res = mongo_async_consume(&mongo, fds[SOCKET].revents)) >= 0);

            if(res == 0)
                continue;

            MONGO_TRY_GENERIC(&mongo)
            {
                bson_iterator it;

                if(!cursor)
                    cursor = mongo_find_response(&mongo, "test.random");

                while(mongo_cursor_next(cursor))
                {
                    if(!bson_find(&it, &cursor->current, "_id"))
                        return -1;

                    if(bson_iterator_type(&it) != bson_bindata)
                        return -1;

                    ++fetches;
                }

                mongo_cursor_destroy(cursor);
                return fetches == SAMPLES ? 0 : -1;
            }
            MONGO_CATCH_GENERIC(&mongo)
            {
                /* We requested new data (write on socket) and wait for response now. */
            }
        }
    }
}
