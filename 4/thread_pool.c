#include "thread_pool.h"
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>

struct thread_task {
    void *(*func)(void *);
    void *arg;
    void *result;
    bool is_finished;
    bool is_pushed;
    bool is_detached;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    struct thread_task *next;
};

struct thread_pool {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int max_threads;
    int num_threads;
    int active_threads;
    struct thread_task *task_queue_head;
    struct thread_task *task_queue_tail;
    bool is_shutdown;
    pthread_t *threads;
};

static void *worker(void *arg);

int thread_pool_new(int max_thread_count, struct thread_pool **pool) {
    if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS)
        return TPOOL_ERR_INVALID_ARGUMENT;

    struct thread_pool *p = malloc(sizeof(struct thread_pool));
    if (!p) return TPOOL_ERR_SYSTEM;

    p->max_threads = max_thread_count;
    p->num_threads = 0;
    p->active_threads = 0;
    p->task_queue_head = NULL;
    p->task_queue_tail = NULL;
    p->is_shutdown = false;
    p->threads = malloc(max_thread_count * sizeof(pthread_t));
    if (!p->threads) {
        free(p);
        return TPOOL_ERR_SYSTEM;
    }

    if (pthread_mutex_init(&p->mutex, NULL) != 0 ||
        pthread_cond_init(&p->cond, NULL) != 0) {
        free(p->threads);
        free(p);
        return TPOOL_ERR_SYSTEM;
    }

    *pool = p;
    return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool) {
    return pool->num_threads;
}

int thread_pool_delete(struct thread_pool *pool) {
    pthread_mutex_lock(&pool->mutex);
    if (pool->task_queue_head != NULL) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_HAS_TASKS;
    }

    pool->is_shutdown = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    for (int i = 0; i < pool->num_threads; i++)
        pthread_join(pool->threads[i], NULL);

    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->cond);
    free(pool->threads);
    free(pool);
    return 0;
}

int thread_task_new(struct thread_task **task, thread_task_f function, void *arg) {
    struct thread_task *t = malloc(sizeof(struct thread_task));
    if (!t) return TPOOL_ERR_SYSTEM;

    t->func = function;
    t->arg = arg;
    t->is_finished = false;
    t->is_pushed = false;
    t->is_detached = false;
    t->next = NULL;
    pthread_mutex_init(&t->mutex, NULL);
    pthread_cond_init(&t->cond, NULL);

    *task = t;
    return 0;
}

int thread_task_delete(struct thread_task *task) {
    pthread_mutex_lock(&task->mutex);
    if (task->is_pushed && !task->is_finished) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_IN_POOL;
    }
    pthread_mutex_unlock(&task->mutex);

    pthread_mutex_destroy(&task->mutex);
    pthread_cond_destroy(&task->cond);
    free(task);
    return 0;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
    if (pool == NULL || task == NULL)
        return TPOOL_ERR_INVALID_ARGUMENT;

    pthread_mutex_lock(&task->mutex);
    if (task->is_pushed || !task->is_finished) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_IN_POOL;
    }
    task->is_finished = false;
    task->is_pushed = true;
    pthread_mutex_unlock(&task->mutex);

    pthread_mutex_lock(&pool->mutex);
    if (pool->num_threads < pool->max_threads && 
        pool->active_threads == pool->num_threads) {
        int err = pthread_create(
            &pool->threads[pool->num_threads], 
            NULL, 
            worker, 
            pool
        );
        if (err) {
            pthread_mutex_unlock(&pool->mutex);
            return TPOOL_ERR_SYSTEM;
        }
        pool->num_threads++;
    }

    if (pool->task_queue_tail) {
        pool->task_queue_tail->next = task;
    } else {
        pool->task_queue_head = task;
    }
    pool->task_queue_tail = task;
    task->next = NULL;

    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

static void *worker(void *arg) {
    struct thread_pool *pool = arg;

    while (true) {
        pthread_mutex_lock(&pool->mutex);
        while (!pool->is_shutdown && pool->task_queue_head == NULL)
            pthread_cond_wait(&pool->cond, &pool->mutex);

        if (pool->is_shutdown) {
            pthread_mutex_unlock(&pool->mutex);
            break;
        }

        struct thread_task *task = pool->task_queue_head;
        if (task) {
            pool->task_queue_head = task->next;
            if (!pool->task_queue_head)
                pool->task_queue_tail = NULL;
        }
        pool->active_threads++;
        pthread_mutex_unlock(&pool->mutex);

        void *result = task->func(task->arg);

        pthread_mutex_lock(&task->mutex);
        task->result = result;
        task->is_finished = true;
        task->is_pushed = false;
        bool is_detached = task->is_detached;
        pthread_cond_broadcast(&task->cond);
        pthread_mutex_unlock(&task->mutex);

        if (is_detached)
            thread_task_delete(task);

        pthread_mutex_lock(&pool->mutex);
        pool->active_threads--;
        pthread_mutex_unlock(&pool->mutex);
    }
    return NULL;
}

int thread_task_join(struct thread_task *task, void **result) {
    if (task == NULL) return TPOOL_ERR_INVALID_ARGUMENT;

    pthread_mutex_lock(&task->mutex);
    if (!task->is_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    while (!task->is_finished)
        pthread_cond_wait(&task->cond, &task->mutex);

    if (result != NULL)
        *result = task->result;
    
    pthread_mutex_unlock(&task->mutex);
    return 0;
}

#if NEED_TIMED_JOIN
int thread_task_timed_join(struct thread_task *task, double timeout, void **result) {
    if (task == NULL) return TPOOL_ERR_INVALID_ARGUMENT;

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += (time_t)timeout;
    ts.tv_nsec += (long)(timeout - (time_t)timeout) * 1e9;
    
    if (ts.tv_nsec >= 1e9) {
        ts.tv_sec++;
        ts.tv_nsec -= 1e9;
    }

    pthread_mutex_lock(&task->mutex);
    if (!task->is_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    while (!task->is_finished) {
        int err = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
        if (err == ETIMEDOUT) {
            pthread_mutex_unlock(&task->mutex);
            return TPOOL_ERR_TIMEOUT;
        }
    }

    if (result != NULL)
        *result = task->result;
    
    pthread_mutex_unlock(&task->mutex);
    return 0;
}
#endif

#if NEED_DETACH
int thread_task_detach(struct thread_task *task) {
    if (task == NULL) return TPOOL_ERR_INVALID_ARGUMENT;

    pthread_mutex_lock(&task->mutex);
    if (!task->is_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }
    
    task->is_detached = true;
    if (task->is_finished) {
        pthread_mutex_unlock(&task->mutex);
        thread_task_delete(task);
    } else {
        pthread_mutex_unlock(&task->mutex);
    }
    return 0;
}
#endif