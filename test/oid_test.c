/* oid.c */

#include "test.h"
#include "mongo.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

int increment( void ) {
    static int i = 1000;
    i++;
    return i;
}

void fuzz(char f[5]) {
    memcpy(f, "hostid_and_pid", 5);
}

/* Test custom increment and fuzz functions. */
int main() {

    bson_oid_t o;
    int res;
    char bigendian_i[4] = {0};

    bson_set_oid_inc( increment );
    bson_set_oid_fuzz( fuzz );

    bson_oid_gen( &o );

    memcpy(bigendian_i + 1, o.bytes + 9, 3);
    bson_big_endian32(&res, bigendian_i);

    ASSERT( !strncmp(o.bytes + 4, "hostid_and_pid", 5)); 
    ASSERT( res == 1001 );

    return 0;
}
