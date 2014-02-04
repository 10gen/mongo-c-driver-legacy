#ifndef SPIN_LOCK_H_
#define SPIN_LOCK_H_

#ifdef _MSC_VER
  #include <windows.h>
#else 
  #include <pthread.h>
#endif 

typedef volatile long spin_lock;

void spinLock_init( spin_lock *_this );
void spinLock_destroy( spin_lock *_this );
void spinLock_lock( spin_lock *_this );
void spinlock_unlock( spin_lock *_this );

#endif