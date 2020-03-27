#include <stdlib.h>
#include <pthread.h>

// circular array
typedef struct _queue {
    int size;
    int used;
    int first;
    void **data;
    pthread_mutex_t *mutex;
    pthread_cond_t *buffer_full;
    pthread_cond_t *buffer_empty;
} _queue;

#include "queue.h"

queue q_create(int size) {
    queue q = malloc(sizeof(_queue));
    q->size  = size;
    q->used  = 0;
    q->first = 0;
    q->mutex =malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(q->mutex,NULL);
    q->buffer_full = malloc(sizeof(pthread_cond_t));
    pthread_cond_init(q->buffer_full,NULL);
    q->buffer_empty  = malloc(sizeof(pthread_cond_t));
    pthread_cond_init(q->buffer_empty,NULL);
    q->data = malloc(size*sizeof(void *));
    return q;
}

int q_elements(queue q) {
    return q->used;
}

int q_insert(queue q, void *elem) {
    //if(q->size == q->used) return 0;
    //q->data[(q->first+q->used) % q->size] = elem;    
    //q->used++; 
    //return 1;
    pthread_mutex_lock(q->mutex);
    while (q->size == q->used) {
	pthread_cond_wait(q->buffer_full,q->mutex);
    }
    q->data[(q->first+q->used) % q->size] = elem;    
    q->used++;
    if (q_elements(q) == 1){pthread_cond_broadcast(q->buffer_empty);}
    pthread_mutex_unlock(q->mutex); 
    return 1;
}

void *q_remove(queue q) {
    void *res;
    pthread_mutex_lock(q->mutex);
    while (q->used==0) {
	pthread_cond_wait(q->buffer_empty,q->mutex);
    }
    res = q->data[q->first];
    q->first = (q->first+1) % q->size;
    q->used--;
    if(q_elements(q) == q->size -1){
       //pthread_cond_wait(q->buffer_empty, q->mutex);
       pthread_cond_broadcast(q->buffer_full);
    }
    pthread_mutex_unlock(q->mutex);
    return res;
}

void q_destroy(queue q) {
    free(q->data);
    free(q->mutex);
    free(q);
}
