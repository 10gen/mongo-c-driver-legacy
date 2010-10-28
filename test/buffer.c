/* test.c */

#include "test.h"
#include "mongo.h"

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>



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

    if(buffer->length < buffer->size >> 1) {
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


int main() {
    size_t k, length;
    char data[512];
    mongo_connection_buffer buffer;
    buffer.data = 0;

    srand(time(0));

    for(k = 0; k < 64; ++k)
    {
        length = rand() % 512;
        fprintf(stderr, "Appending %lu bytes of data... ", length);
        mongo_buffer_append(&buffer, data, length);
        fprintf(stderr, " offset: %lu, length: %lu, size: %lu\n", buffer.offset, buffer.length, buffer.size);

        if((k & 1) == 0) {
            length = rand() % 512;

            fprintf(stderr, "Erasing %lu bytes of data... ", length);
            mongo_buffer_erase(&buffer, length);
            fprintf(stderr, " offset: %lu, length: %lu, size: %lu\n", buffer.offset, buffer.length, buffer.size);
        }
    }

    return 0;
}
