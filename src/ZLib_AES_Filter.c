#include "bson.h"
#include "md5.h"
#include "gridfs.h"
#include "ZLib_AES_Filter.h"

/* ZLib libraries @ https://github.com/madler/zlib.git */
#include "zlib.h"
/* Fast rijndael @ https://github.com/Convey-Compliance/fast-aes.git */
#include "rijndael-alg-fst.h"

#define AES_BLOCK_SIZE 16
#define AES_CRYPTO_KEY_SIZE 32

typedef struct {
  FILTER_CONTEXT_CALLBACKS; /* Base "Class" expansion */
  void* targetBuffer; /* Special general purpose buffer, for Zlib and Decryption on certain cases */
  size_t targetBuffer_size; /* Allocated buffer size */
  void* targetDecryptBuffer; /* Dedicated buffer for decryption operations */ 
  size_t targetDecryptBuffer_size; /* Allocated decryption buffer size */
  int key_bits; 
  char crypto_key[AES_CRYPTO_KEY_SIZE]; /* Up to AES 256 bits Encryption vector */
} ZLib_AES_filterContext;

#define ALIGN_TO_AES_BLOCK_SIZE(N) N = ( (N + AES_BLOCK_SIZE - 1) & ~(AES_BLOCK_SIZE - 1) )

/* Accesor Macros */
#define CRYPTO_KEY(context) ((ZLib_AES_filterContext*)context)->crypto_key
#define TARGET_BUFFER(context) ((ZLib_AES_filterContext*)context)->targetBuffer
#define TARGET_BUFFER_SIZE(context) ((ZLib_AES_filterContext*)context)->targetBuffer_size
#define DECRYPT_BUFFER(context) ((ZLib_AES_filterContext*)context)->targetDecryptBuffer
#define TARGET_DECRYPTBUFFER_SIZE(context) ((ZLib_AES_filterContext*)context)->targetDecryptBuffer_size
#define KEY_BITS(context) ((ZLib_AES_filterContext*)context)->key_bits

/* General macros */
#define NEXT_AES_BLOCK(target) (u8*)target += AES_BLOCK_SIZE

/* ----------------- */
/* Private functions */
/* ----------------- */

static char* bufferFromContext( void* context, size_t tmpLen, int flags ) {
  if( TARGET_BUFFER_SIZE(context) < tmpLen && TARGET_BUFFER(context) != NULL ) {
    bson_free( TARGET_BUFFER(context) );
    TARGET_BUFFER(context) = NULL;
  }
  if( TARGET_BUFFER(context) == NULL ) {
    TARGET_BUFFER(context) = bson_malloc( tmpLen );  
    TARGET_BUFFER_SIZE(context) = tmpLen;
  }
  return (char*)TARGET_BUFFER(context);
}

#define XOR_AES_BLOCK(TARGET, SOURCE) {\
  *(__int64*)(TARGET) ^= *(__int64*)(SOURCE); \
  *(__int64*)((TARGET) + sizeof(__int64)) ^= *(__int64*)((SOURCE) + sizeof(__int64)); \
}

static int Zlib_AES_PreProcessChunk(void* context, char** targetBuf, size_t* targetLen, const char* srcBuf, size_t srcLen, int flags) {
  uLongf tmpLen = compressBound( DEFAULT_CHUNK_SIZE ) + AES_BLOCK_SIZE;
  
  ALIGN_TO_AES_BLOCK_SIZE( tmpLen ); /* Let's make sure we have enough space for AES encryption of full blocks */
  if( flags & GRIDFILE_COMPRESS || flags & GRIDFILE_ENCRYPT ) {    
    *targetBuf = bufferFromContext( context, tmpLen, flags );    
    
    if( flags & GRIDFILE_COMPRESS ) {
      if( compress2( (Bytef*)(*targetBuf), &tmpLen, (Bytef*)srcBuf, (uLong)srcLen, Z_BEST_SPEED ) != Z_OK ) return -1;
    } else {
      tmpLen = (uLongf)srcLen;
      memmove( *targetBuf, srcBuf, srcLen ); /* We need to move the data to targetBuf to implement CBC encryption */
    }
    
    if( flags & GRIDFILE_ENCRYPT ) {      
      u8* target = (u8*)*targetBuf;       
      u32 rk[4 * (MAXNR + 1)];
      u8 lastblock[AES_BLOCK_SIZE];
      u8 lastBlockLen = tmpLen % AES_BLOCK_SIZE;
      int r = rijndaelKeySetupEnc( rk, (const u8*)CRYPTO_KEY( context ), KEY_BITS( context ) ); 
      
      if( tmpLen > AES_BLOCK_SIZE ) { 
        size_t loops = tmpLen / AES_BLOCK_SIZE;

        XOR_AES_BLOCK( target, CRYPTO_KEY( context ) ); /* Initial CBC step using initialization vector/crypto key */
        while( loops > 0 ) {
          rijndaelEncrypt( rk, r, target, target );        
          if( --loops > 0 ) XOR_AES_BLOCK( target + AES_BLOCK_SIZE, target ); /* CBC second block and on... */       
          NEXT_AES_BLOCK( target );
        }      
      }
      memset( lastblock + lastBlockLen, 0, sizeof( u8 ) * AES_BLOCK_SIZE - lastBlockLen );
      if( lastBlockLen > 0 ) {
        memmove( lastblock, target, lastBlockLen );
        lastblock[AES_BLOCK_SIZE - 1] = lastBlockLen;
      }
      if( tmpLen > AES_BLOCK_SIZE ) XOR_AES_BLOCK( lastblock, target - AES_BLOCK_SIZE ) /* CBC last block with prior block before encrypting ONLY if source > AES block size*/
      else XOR_AES_BLOCK( lastblock, CRYPTO_KEY( context ) ); /* Otherwise CBC last block with initialization vector */
      rijndaelEncrypt( rk, r, lastblock, target ); 
      if( lastBlockLen == 0) tmpLen += AES_BLOCK_SIZE;
      else ALIGN_TO_AES_BLOCK_SIZE( tmpLen );
    }
    *targetLen = (size_t)tmpLen;
  } else {
    *targetBuf = (char*)srcBuf;
    *targetLen = srcLen;
  }
  return 0;
}

static void* decryptBufferFromContext( void* context, size_t bufLen ) {
  if( TARGET_DECRYPTBUFFER_SIZE(context) < bufLen && DECRYPT_BUFFER(context) != NULL ) {
    bson_free( DECRYPT_BUFFER(context) );
    DECRYPT_BUFFER(context) = NULL;
  }
  if( DECRYPT_BUFFER(context) == NULL ) {
    DECRYPT_BUFFER(context) = bson_malloc( bufLen );  
    TARGET_DECRYPTBUFFER_SIZE(context) = bufLen;
  }
  return DECRYPT_BUFFER(context);
}

static int Zlib_AES_PostProcessChunk(void* context, char** targetBuf, size_t* targetLen, const char* srcData, size_t srcLen, int flags) {   
  uLongf tmpLen = DEFAULT_CHUNK_SIZE + AES_BLOCK_SIZE; 

  ALIGN_TO_AES_BLOCK_SIZE( tmpLen ); /* Let's make sure we have enough space for AES decryption of full blocks */
  if( flags & GRIDFILE_COMPRESS || flags & GRIDFILE_ENCRYPT ) {          
    if( flags & GRIDFILE_COMPRESS ) *targetBuf = bufferFromContext( context, tmpLen, flags );
    if( flags & GRIDFILE_ENCRYPT ) {
      u8* target = (u8*)decryptBufferFromContext( context, tmpLen );
      u8* source = (u8*)srcData;
      size_t n_loop = 0;
      size_t loops = srcLen / AES_BLOCK_SIZE; /* We KNOW the number of blocks is multiple of AES_BLOCK_SIZE */
      u32 rk[4 * (MAXNR + 1)];
      int r = rijndaelKeySetupDec( rk, (const u8*)CRYPTO_KEY( context ), KEY_BITS( context ) ); 
            
      while( n_loop < loops ) {
        rijndaelDecrypt( rk, r, source, target );                  
        if( n_loop++ > 0 ) XOR_AES_BLOCK( target, source - AES_BLOCK_SIZE ) /* CBC second block and on... */           
        else XOR_AES_BLOCK( target, CRYPTO_KEY( context ) );
        NEXT_AES_BLOCK( source );
        NEXT_AES_BLOCK( target );
      }
      srcLen -= AES_BLOCK_SIZE - *(target - 1); /* Let's obtain the REAL length of the last block from the last byte of the block and adjust srcLen */
    }
    
    if( flags & GRIDFILE_COMPRESS ) {
      Bytef* source = flags & GRIDFILE_ENCRYPT ? (Bytef*)DECRYPT_BUFFER(context) : (Bytef*)srcData;        
       
      if (uncompress( (Bytef*)(*targetBuf), &tmpLen, source, (uLong)srcLen ) != Z_OK ) return -1;      
      *targetLen = (size_t)tmpLen;
    } else {
      *targetLen = srcLen;
      *targetBuf = (char*)DECRYPT_BUFFER(context); 
    }    
  } else {
    *targetBuf = (char*)srcData;
    *targetLen = srcLen;
  }
  return 0;
}

static size_t Zlib_AES_PendingDataNeededSize (void* context, int flags) {
  if( flags & GRIDFILE_COMPRESS ) return compressBound( DEFAULT_CHUNK_SIZE );
  else return DEFAULT_CHUNK_SIZE;  
}

static void ZLib_AES_reset_context(void* context, int flags){
  if( TARGET_BUFFER(context) ) {
    bson_free( TARGET_BUFFER(context) );
    TARGET_BUFFER(context) = NULL;
    TARGET_BUFFER_SIZE(context) = 0;
  }
  if( DECRYPT_BUFFER(context) ) {
    bson_free( DECRYPT_BUFFER(context) );
    DECRYPT_BUFFER(context) = NULL;
    TARGET_DECRYPTBUFFER_SIZE(context) = 0;
  }
}

static ZLib_AES_filterContext default_zlib_filter;

static void ZLib_AES_init_context(ZLib_AES_filterContext* context) {
  context->pending_data_buffer_size = &Zlib_AES_PendingDataNeededSize;
  context->read_filter = &Zlib_AES_PostProcessChunk;
  context->write_filter = &Zlib_AES_PreProcessChunk;
  context->reset_context = &ZLib_AES_reset_context;
  context->targetBuffer = NULL;
  context->targetBuffer_size = 0;
  context->targetDecryptBuffer = NULL;
  context->targetDecryptBuffer_size = 0;
  memset(context->crypto_key, 0, sizeof(context->crypto_key));
  context->key_bits = 0;
}

/* -------------------- */
/*   Public functions   */
/* -------------------- */

MONGO_EXPORT int init_ZLib_AES_filtering( int flags ){
  ZLib_AES_init_context(&default_zlib_filter);
  set_global_filter_context( (filterContext*)&default_zlib_filter );  
  return 0;
}

MONGO_EXPORT void* create_ZLib_AES_filter_context( int flags ){
  ZLib_AES_filterContext* context = (ZLib_AES_filterContext*)bson_malloc( sizeof( ZLib_AES_filterContext ) );
  ZLib_AES_init_context( context );
  return context;
}

MONGO_EXPORT void destroy_ZLib_AES_filter_context( void* context, int flags ){
  ZLib_AES_reset_context( context, flags );  
  bson_free( context );
}

MONGO_EXPORT int ZLib_AES_filter_context_set_encryption_key( void* context, const char* passphrase, int bits ){
  mongo_md5_state_t md5_state;
  size_t ori_len = strlen( passphrase );
  size_t len;

  if( ori_len < 2 && bits != AES_128 ) return -1; /* Passphrase must be at least 2 chars long if bits > 128 */    
  switch( bits ) {
    case AES_128: { len = ori_len; break; }
    case AES_192: 
    case AES_256: { len = ori_len / 2; break; }
    default : return -1;
  }

  KEY_BITS( context ) = bits;
  mongo_md5_init( &md5_state );
  mongo_md5_append( &md5_state, (const mongo_md5_byte_t *)passphrase, (int)len ); 
  mongo_md5_finish( &md5_state, (mongo_md5_byte_t*)CRYPTO_KEY( context ) );
  if( bits == AES_128 ) return 0;
  
  mongo_md5_init( &md5_state );
  mongo_md5_append( &md5_state, (const mongo_md5_byte_t *)&(passphrase[len]), (int)(ori_len - len) );
  switch( bits ) {
    case AES_192: {
      /* The following line is to avoid loosing the last 8 bytes of the first pass of the MD5 hash
         when using 192 bits. We will overwrite these bytes on the next call to mongo_md5_finish 
         for the second pass of the passphrase, but by making those 8 bytes part of the key we don't loose 
         them and we still get a legit 192 bits created from the passphrase */
      mongo_md5_append( &md5_state, (const mongo_md5_byte_t *)&(CRYPTO_KEY( context )[8]), 8);       
      mongo_md5_finish( &md5_state, (mongo_md5_byte_t*)&(CRYPTO_KEY( context )[8]));
      break;
    }
    case AES_256: mongo_md5_finish( &md5_state, (mongo_md5_byte_t*)&(CRYPTO_KEY( context )[16]));      
  } 
  return 0;
}

