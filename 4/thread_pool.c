#include "thread_pool.h"
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h>
#include <errno.h>


#define UNUSED(x) (void)(x)

struct thread_task {
    void *(*func)(void *);
    void *arg;
    void *result;
    bool is_finished;
    bool is_joined;
    bool is_pushed;
    bool is_detached;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

struct thread_pool {
    pthread_t *threads;
    int max_threads;
    int active_threads;
    struct thread_task **task_queue;
    int task_count;
    int task_cap;
    bool is_shutdown;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

static void *worker(void *arg);

int thread_pool_new(int max_thread_count, struct thread_pool **pool) {
    if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS)
        return TPOOL_ERR_INVALID_ARGUMENT;

    struct thread_pool *p = calloc(1, sizeof(*p));
    if (!p) return TPOOL_ERR_NOT_ENOUGH_MEMORY;

    p->threads = calloc(max_thread_count, sizeof(pthread_t));
    p->task_queue = calloc(TPOOL_MAX_TASKS, sizeof(struct thread_task *));
    if (!p->threads || !p->task_queue) {
        free(p->threads);
        free(p->task_queue);
        free(p);
        return TPOOL_ERR_NOT_ENOUGH_MEMORY;
    }

    pthread_mutex_init(&p->mutex, NULL);
    pthread_cond_init(&p->cond, NULL);
    p->task_cap = TPOOL_MAX_TASKS;
    p->max_threads = max_thread_count;

    for (int i = 0; i < max_thread_count; i++) {
        pthread_create(&p->threads[i], NULL, worker, p);
    }

    *pool = p;
    return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool) {
    return pool->active_threads;
}

int thread_pool_delete(struct thread_pool *pool) {
    pthread_mutex_lock(&pool->mutex);
    if (pool->task_count > 0) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_HAS_TASKS;
    }

    pool->is_shutdown = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    for (int i = 0; i < pool->max_threads; i++) {
        pthread_join(pool->threads[i], NULL);
    }

    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->cond);
    free(pool->task_queue);
    free(pool->threads);
    free(pool);
    return 0;
}

int thread_task_new(struct thread_task **task, void *(*func)(void *), void *arg) {
    if (!task || !func)
        return TPOOL_ERR_INVALID_ARGUMENT;

    struct thread_task *t = calloc(1, sizeof(*t));
    if (!t) return TPOOL_ERR_NOT_ENOUGH_MEMORY;

    t->func = func;
    t->arg = arg;
    pthread_mutex_init(&t->mutex, NULL);
    pthread_cond_init(&t->cond, NULL);
    *task = t;
    return 0;
}

int thread_task_delete(struct thread_task *task) {
    if (task->is_pushed && !task->is_finished)
        return TPOOL_ERR_TASK_IN_POOL;

    if (!task->is_joined && !task->is_detached && task->is_pushed)
        return TPOOL_ERR_TASK_IN_POOL;

    pthread_mutex_destroy(&task->mutex);
    pthread_cond_destroy(&task->cond);
    free(task);
    return 0;
}

int thread_task_join(struct thread_task *task, void **result) {
    if (!task->is_pushed)
        return TPOOL_ERR_TASK_NOT_PUSHED;

    pthread_mutex_lock(&task->mutex);
    while (!task->is_finished)
        pthread_cond_wait(&task->cond, &task->mutex);
    if (result)
        *result = task->result;
    task->is_joined = true;
    pthread_mutex_unlock(&task->mutex);
    return 0;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
    pthread_mutex_lock(&pool->mutex);
    if (pool->task_count >= pool->task_cap) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_TOO_MANY_TASKS;
    }

    pool->task_queue[pool->task_count++] = task;
    task->is_pushed = true;
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

int thread_task_timed_join(struct thread_task *task, double timeout, void **result) {
#if NEED_TIMED_JOIN
    if (!task->is_pushed)
        return TPOOL_ERR_TASK_NOT_PUSHED;

    pthread_mutex_lock(&task->mutex);
    if (task->is_finished) {
        if (result)
            *result = task->result;
        task->is_joined = true;
        pthread_mutex_unlock(&task->mutex);
        return 0;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    time_t sec = (time_t)timeout;
    long nsec = (long)((timeout - sec) * 1e9);
    ts.tv_sec += sec;
    ts.tv_nsec += nsec;
    if (ts.tv_nsec >= 1e9) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1e9;
    }

    int rc = 0;
    while (!task->is_finished && rc != ETIMEDOUT)
        rc = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);

    if (task->is_finished) {
        if (result)
            *result = task->result;
        task->is_joined = true;
        pthread_mutex_unlock(&task->mutex);
        return 0;
    }

    pthread_mutex_unlock(&task->mutex);
    return TPOOL_ERR_TIMEOUT;
#else
    (void)task; (void)timeout; (void)result;
    return TPOOL_ERR_NOT_IMPLEMENTED;
#endif
}

int thread_task_detach(struct thread_task *task) {
#if NEED_DETACH
    if (!task->is_pushed)
        return TPOOL_ERR_TASK_NOT_PUSHED;
    task->is_detached = true;
    return 0;
#else
    (void)task;
    return TPOOL_ERR_NOT_IMPLEMENTED;
#endif
}

static void *worker(void *arg) {
    struct thread_pool *pool = arg;

    pthread_mutex_lock(&pool->mutex);
    pool->active_threads++;
    pthread_mutex_unlock(&pool->mutex);

    while (true) {
        pthread_mutex_lock(&pool->mutex);
        while (pool->task_count == 0 && !pool->is_shutdown)
            pthread_cond_wait(&pool->cond, &pool->mutex);
        if (pool->is_shutdown && pool->task_count == 0) {
            pthread_mutex_unlock(&pool->mutex);
            break;
        }

        struct thread_task *task = pool->task_queue[0];
        for (int i = 1; i < pool->task_count; i++)
            pool->task_queue[i - 1] = pool->task_queue[i];
        pool->task_count--;
        pthread_mutex_unlock(&pool->mutex);

        void *res = task->func(task->arg);

        pthread_mutex_lock(&task->mutex);
        task->result = res;
        task->is_finished = true;
        pthread_cond_broadcast(&task->cond);
        pthread_mutex_unlock(&task->mutex);

#if NEED_DETACH
        if (task->is_detached)
            thread_task_delete(task);
#endif
    }

    pthread_mutex_lock(&pool->mutex);
    pool->active_threads--;
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
}
