#include "spin_lock.h"

#define SPINLOCK_LOCKED 1
#define SPINLOCK_UNLOCKED 0
#define SPINS_BETWEEN_THREADSWITCH 1000

static long crossSwap( spin_lock *_this, long originalValue, long exchgValue ) {
#ifdef _MSC_VER
  return InterlockedCompareExchange( _this, exchgValue, originalValue );
#else
  return __sync_val_compare_and_swap( _this, originalValue, exchgValue );
#endif
}

static void crossYield( void ) {
#ifdef _MSC_VER
  SwitchToThread();
#else
  sched_yield();
#endif
}

static void spin( int *spinCount ) {
  if( (*spinCount)++ > SPINS_BETWEEN_THREADSWITCH ) {
    crossYield();
    *spinCount = 0;
  }
}

void spinLock_init( spin_lock *_this ){
  (*_this) = SPINLOCK_UNLOCKED; /* Start unlocked */
}

void spinLock_destroy( spin_lock *_this ){
  /* Empty function, in the future can be used if spin_lock becomes a more complex type
     that requires some kind of finalization */
}

void spinLock_lock( spin_lock *_this ) {
  int spins = 0;
  while( crossSwap( _this, SPINLOCK_UNLOCKED, SPINLOCK_LOCKED ) != SPINLOCK_UNLOCKED ) {
    spin( &spins );  
  };
}

void spinlock_unlock( spin_lock *_this ) {
  *_this = SPINLOCK_UNLOCKED;
}