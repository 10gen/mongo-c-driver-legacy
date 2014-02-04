#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include "mongo.h"
#include "spin_lock.h"

#define MAX_USER_LEN 256
#define MAX_PASS_LEN 256
#define MAX_DB_LEN 256
#define MAX_REPLICA_NAME_LEN 256

typedef enum mongo_connection_error_t {
    MONGO_CONNECTION_SUCCESS = 0,                   /**< Connection success! */
    MONGO_CONNECTION_INVALID_CONNECTION_STRING,     /**< Connection string is invalid */
    MONGO_CONNECTION_AUTH_FAIL,                     /**< Invalid user or pass */
    MONGO_CONNECTION_MONGO_ERROR                    /**< Mongo driver error */
} mongo_connection_error_t;

typedef struct mongo_connection {
    mongo conn[1];                        /**< mongo object */
    mongo_connection_error_t err;         /**< error field */
    struct mongo_connection *next;        /**< pointer to next connection in the pool */
    struct mongo_connection_pool *pool;   /**< pointer to connection pool(used to return connection in the pool after it is released and to get connection string) */
} mongo_connection;

typedef struct mongo_connection_pool {
    char *cs;                             /**< connection string, see http://docs.mongodb.org/manual/reference/connection-string/ note: only replicaSet option is supported */
    spin_lock lock;                       /**< spin lock object */
    mongo_connection *head;               /**< first connection in the pool */
    struct mongo_connection_pool *next;   /**< next pool in dictionary */
} mongo_connection_pool;

typedef struct mongo_connection_dictionary {
    mongo_connection_pool *head;        /**< first pool in dictionary */
    spin_lock lock;                        /**< spin lock object*/
} mongo_connection_dictionary;

/**
 * connect and authenticate
 *
 * @param conn connection object
 *
 * @return MONGO_OK if success, MONGO_ERROR if failed
 */
MONGO_EXPORT int mongo_connection_connect( mongo_connection *conn );

/**
 * re-connect and authenticate
 *
 * @param conn connection object
 *
 * @return MONGO_OK if success, MONGO_ERROR if failed
 */
MONGO_EXPORT int mongo_connection_reconnect( mongo_connection *conn );

/**
 * disconnect
 *
 * @param conn connection object
 */
MONGO_EXPORT void mongo_connection_disconnect( mongo_connection *conn );

/**
 * get first connection from pool or open new one(if no connection in the pool)
 *
 * @param pool connection pool to get connection from
 *
 * @return connection already connected. Not need to call mongo_connection_connect()
 * note: connection could be not connected(check errors)
 */
MONGO_EXPORT mongo_connection* mongo_connection_pool_acquire( mongo_connection_pool *pool );

/**
 * put connection back in the pool
 *
 * @param pool connection pool to return connection
 *
 * @param conn unused connection
 */
MONGO_EXPORT void mongo_connection_pool_release( mongo_connection_pool *pool, mongo_connection *conn );

/**
 * initialize dictionary, should be called first
 *
 * @param dict dictionary of connection pools
 */
MONGO_EXPORT void mongo_connection_dictionary_init( mongo_connection_dictionary *dict );

/**
 * Close any existing connection to the server and free all allocated
 * memory associated with the dict object
 *
 * @param dict dictionary of connection pools
 */
MONGO_EXPORT void mongo_connection_dictionary_destroy( mongo_connection_dictionary *dict );

/**
 * get pool by connection string or create new one(will be added to dictionary)
 *
 * @param dict dictionary of connection pools(one for each connection string)
 *
 * @param cs connection string
 *
 * @return connection pool object
 */
MONGO_EXPORT mongo_connection_pool* mongo_connection_dictionary_get_pool( mongo_connection_dictionary *dict, const char *cs );

#endif