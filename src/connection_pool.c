#include "connection_pool.h"
#include "env.h"

static int connectToReplicaSet( mongo *conn, const char *replicaName, char *hosts ) {
  char *hostPortPair = strtok( hosts, "," ), host[MAXHOSTNAMELEN];
  int port = MONGO_DEFAULT_PORT;

  mongo_replica_set_init( conn, replicaName );
  while( hostPortPair != NULL ) {
    sscanf( hostPortPair, "%[^:]:%d", host, &port );
    mongo_replica_set_add_seed( conn, host, port );
    hostPortPair = strtok( NULL, "," );
    port = MONGO_DEFAULT_PORT; /* reset port to default */
  }
  return mongo_replica_set_client( conn );
}

/* mongo_connection methods */

void mongo_connection_set_socket_timeout( mongo_connection *conn, unsigned int timeout )
{
  conn->timeout = timeout;
  if( conn->conn->connected )
  {
    mongo_env_set_socket_op_timeout( conn->conn, timeout );
  }
}

unsigned int mongo_connection_get_socket_timeout( mongo_connection *conn )
{
  return conn->timeout;
}

static mongo_connection* mongo_connection_new() {
  return ( mongo_connection* )bson_malloc( sizeof( mongo_connection ) );
}

static void mongo_connection_delete( mongo_connection* _this ) {
  mongo_destroy( _this->conn );
  bson_free( _this );
}

static int mongo_connection_authenticate( mongo_connection *_this, const char *connectionString ) {
  char user[MAX_USER_LEN] = {'\0'}, pass[MAX_PASS_LEN] = {'\0'}, db[MAX_DB_LEN] = {'\0'};
  int credentialsScanned = sscanf( connectionString, "mongodb://%[^:]:%[^@]@%*[^/]/%[^?]?", user, pass, db );

  if( credentialsScanned != 3 ) {
    /* not all of required credentials exists */
    _this->err = MONGO_CONNECTION_INVALID_CONNECTION_STRING;
    return MONGO_ERROR;
  }
  if( mongo_cmd_authenticate( _this->conn, db, user, pass ) == MONGO_ERROR ) {
    _this->err = MONGO_CONNECTION_AUTH_FAIL;
    return MONGO_ERROR;
  }
  return MONGO_OK;
}

static int isNeedToAuth( const char *connectionString ) {
  return strchr( connectionString, '@' ) != NULL;
}

MONGO_EXPORT int mongo_connection_connect( mongo_connection *_this ) {
  int multipleHostsProvided, needToAuth, res;
  char *hosts, replicaName[MAX_REPLICA_NAME_LEN] = {'\0'};

  if( _this->conn->connected == 1 ) return MONGO_OK;

  hosts = ( char* )bson_malloc( sizeof(char) * strlen( _this->pool->cs ) );
  hosts[0] = '\0';
  sscanf( _this->pool->cs, "mongodb://%[^/]/%*[^?]?replicaSet=%s", hosts, replicaName );
  needToAuth = isNeedToAuth( hosts ); /* Moved out of conditional to avoid MSVC warnings... bummer */
  if( needToAuth )
  {
    /* remove user and pass */
    strcpy( hosts, strchr( hosts, '@' ) + 1 );
  }
  multipleHostsProvided = strchr( hosts, ',' ) != NULL;

  do {
    if( hosts[0] == '\0' || /* required */
      ( multipleHostsProvided && replicaName[0] == '\0' )) /* replica set name required if multiple hosts specified */
    {
      mongo_init( _this->conn ); /* reserve resources to allow free without errors while pool will be destroyed */
      _this->err = MONGO_CONNECTION_INVALID_CONNECTION_STRING;
      res = MONGO_ERROR;
      break;
    }

    if( !multipleHostsProvided )
    {
      /* single server */
      char host[MAXHOSTNAMELEN];
      int port = MONGO_DEFAULT_PORT;

      sscanf( hosts, "%[^:]:%d", host, &port );
      res = mongo_client( _this->conn, host, port );
    }
    else
    {
      res = connectToReplicaSet( _this->conn, replicaName, hosts );
    }
    if( res == MONGO_ERROR )
      _this->err = MONGO_CONNECTION_MONGO_ERROR;
    else 
    {
      mongo_env_set_socket_op_timeout( _this->conn, _this->timeout );
      if( needToAuth )
        res = mongo_connection_authenticate( _this, _this->pool->cs );
    }
  } while( 0 );

  bson_free( hosts );
  return res;
}

MONGO_EXPORT int mongo_connection_reconnect( mongo_connection *_this ) {
  if( mongo_reconnect( _this->conn ) == MONGO_OK ) {
    mongo_env_set_socket_op_timeout( _this->conn, _this->timeout );
    if( isNeedToAuth( _this->pool->cs ) && mongo_connection_authenticate( _this, _this->pool->cs ) != MONGO_OK )
      return MONGO_ERROR;
    return MONGO_OK;
  }
  
  return MONGO_ERROR;
}

MONGO_EXPORT void mongo_connection_disconnect( mongo_connection *_this ) {
  mongo_disconnect( _this->conn );
}

/* mongo_connection_pool methods */

static mongo_connection_pool* mongo_connection_pool_new( const char *cs ) {
  mongo_connection_pool *pool = ( mongo_connection_pool* )bson_malloc( sizeof( mongo_connection_pool ) );
  pool->cs = ( char* )bson_malloc( sizeof( char ) * strlen( cs ) + 1 );
  spinLock_init( &pool->lock );
  strcpy( pool->cs, cs );
  return pool;
}

static void mongo_connection_pool_delete( mongo_connection_pool *_this ) {
  mongo_connection *conn = _this->head;
  while( conn != NULL) {
    mongo_connection *next = conn->next;
    mongo_connection_delete( conn );
    conn = next;
  }
  bson_free( _this->cs );
  spinLock_destroy( &_this->lock );
  bson_free( _this );
}

static mongo_connection* mongo_connection_pool_removeFirst( mongo_connection_pool *_this ) {
  mongo_connection *res;

  spinLock_lock( &_this->lock );
  
  res = _this->head;
  if( res != NULL ) {
    _this->head = res->next;
  }

  spinlock_unlock( &_this->lock );  

  return res;
}

MONGO_EXPORT mongo_connection* mongo_connection_pool_acquire( mongo_connection_pool *_this ) {
  mongo_connection *res = mongo_connection_pool_removeFirst( _this );
  if( res != NULL ) return res;
  /* create new connection */
  res = mongo_connection_new();
  res->pool = _this;
  res->err = MONGO_CONNECTION_SUCCESS;
  res->conn->connected = 0; /* This flag will force following code to initialize connection object */
  res->timeout = DEFAULT_SOCKET_TIMEOUT;
  mongo_connection_connect( res );
  res->next = NULL;

  return res;
}

MONGO_EXPORT void mongo_connection_pool_release( mongo_connection_pool *_this, mongo_connection *conn ) {
  spinLock_lock( &_this->lock );

  /* insert at the beginning of the pool */
  conn->next = _this->head;
  _this->head = conn;

  spinlock_unlock( &_this->lock );  
}

/* mongo_connection_dictionary methods */

MONGO_EXPORT void mongo_connection_dictionary_init( mongo_connection_dictionary *_this ) {
  _this->head = NULL;
  spinLock_init( &_this->lock );  
}

MONGO_EXPORT void mongo_connection_dictionary_destroy( mongo_connection_dictionary *_this ) {
  mongo_connection_pool *pool = _this->head;
  while( pool != NULL ) {
    mongo_connection_pool *next = pool->next;
    mongo_connection_pool_delete( pool );
    pool = next;
  }
  spinLock_destroy( &_this->lock );
}

static void mongo_connection_dictionary_addToDictionary( mongo_connection_dictionary *_this, mongo_connection_pool *pool, mongo_connection_pool *lastPoolInDict ) 
{
  /* insert at the end of dictionary */
  if( _this->head == NULL )
    _this->head = pool;
  else
    lastPoolInDict->next = pool;  
}

MONGO_EXPORT mongo_connection_pool* mongo_connection_dictionary_get_pool( mongo_connection_dictionary *_this, const char *cs ) {
  mongo_connection_pool *pool, *lastPoolInDict = NULL;

  /* Let's lock for the whole process of obtaining a pool and adding a new pool to the dictionary. The assumption is that this method
     is not going to be called very often, therefore we can assume low contention. 
     If we use a finer lock only when adding to dictionary we risk adding same pool twice. If this proves troublesome due to high 
     contention we can change logic to finer lock inside adding to dictionary only or coarse critical section based locking */
  spinLock_lock( &_this->lock );    

  /* find in dictionary by connection string */
  for( pool = _this->head; pool != NULL; pool = pool->next ) {
    lastPoolInDict = pool;
    if( strcmp( cs, pool->cs ) == 0 ) break; /* found */    
  }

  if( pool == NULL ) {
    /* create new pool object */
    pool = mongo_connection_pool_new( cs );
    pool->head = NULL;
    pool->next = NULL;    

    mongo_connection_dictionary_addToDictionary( _this, pool, lastPoolInDict );
  }

  spinlock_unlock( &_this->lock );

  return pool;
}