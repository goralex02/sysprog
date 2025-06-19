#include "thread_pool.h"
#include <pthread.h>
#include <stdlib.h>
#include <errno.h>
#include <stdbool.h>
#include <time.h>
#include <string.h>

enum task_state {
    TASK_CREATED,
    TASK_QUEUED,
    TASK_RUNNING,
    TASK_FINISHED
};

struct thread_task {
    thread_task_f function;
    void *arg;
    enum task_state state;
    void *result;
    bool pushed;
    bool joined;
    bool detached;
    struct thread_task *next;
    struct thread_pool *pool;
    pthread_mutex_t task_mutex;
    pthread_cond_t task_cond;
};

struct thread_pool {
    int max_threads;
    pthread_t *threads;
    int thread_count;
    struct thread_task *head;
    struct thread_task *tail;
    int task_count;
    int running_count;
    int idle_count;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool shutdown;
};

static void *worker_thread(void *arg)
{
    struct thread_pool *pool = arg;
    
    pthread_mutex_lock(&pool->mutex);
    pool->idle_count++;
    while (!pool->shutdown) {
        while (pool->head == NULL && !pool->shutdown) {
            pthread_cond_wait(&pool->cond, &pool->mutex);
        }
        if (pool->shutdown) break;

        struct thread_task *task = pool->head;
        pool->head = task->next;
        if (!pool->head) pool->tail = NULL;
        pool->task_count--;
        pool->running_count++;
        pool->idle_count--;
        pthread_mutex_unlock(&pool->mutex);

        void *res = task->function(task->arg);

        pthread_mutex_lock(&pool->mutex);
        pool->running_count--;
        pthread_cond_signal(&pool->cond);
        pool->idle_count++;
        pthread_mutex_unlock(&pool->mutex);

        pthread_mutex_lock(&task->task_mutex);
        task->result = res;
        task->state = TASK_FINISHED;
        bool should_delete = task->detached;
        pthread_cond_broadcast(&task->task_cond);
        pthread_mutex_unlock(&task->task_mutex);

        if (should_delete) {
            pthread_mutex_destroy(&task->task_mutex);
            pthread_cond_destroy(&task->task_cond);
            free(task);
        }

        pthread_mutex_lock(&pool->mutex);
    }
    
    pool->idle_count--;
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
}

int thread_pool_new(int max_thread_count, struct thread_pool **pool)
{
    if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS || pool == NULL)
        return TPOOL_ERR_INVALID_ARGUMENT;

    struct thread_pool *p = malloc(sizeof(*p));
    if (!p) return TPOOL_ERR_SYSTEM;
    p->threads = calloc(max_thread_count, sizeof(pthread_t));
    if (!p->threads) { free(p); return TPOOL_ERR_SYSTEM; }

    p->max_threads = max_thread_count;
    p->thread_count = 0;
    p->head = p->tail = NULL;
    p->task_count = p->running_count = p->idle_count = 0;
    p->shutdown = false;
    pthread_mutex_init(&p->mutex, NULL);
    pthread_cond_init(&p->cond, NULL);
    *pool = p;
    return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool)
{
    if (!pool) return 0;
    pthread_mutex_lock((pthread_mutex_t*)&pool->mutex);
    int c = pool->thread_count;
    pthread_mutex_unlock((pthread_mutex_t*)&pool->mutex);
    return c;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
    if (!pool || !task) return TPOOL_ERR_INVALID_ARGUMENT;
    pthread_mutex_lock(&pool->mutex);
    if (pool->shutdown) { pthread_mutex_unlock(&pool->mutex); return TPOOL_ERR_INVALID_ARGUMENT; }
    if (pool->task_count + pool->running_count >= TPOOL_MAX_TASKS) { pthread_mutex_unlock(&pool->mutex); return TPOOL_ERR_TOO_MANY_TASKS; }

    task->next = NULL; task->pool = pool;
    if (pool->tail) pool->tail->next = task; else pool->head = task;
    pool->tail = task;
    pool->task_count++;

    pthread_mutex_lock(&task->task_mutex);
    task->pushed = true;
    task->state = TASK_QUEUED;
    task->detached = false;
    pthread_mutex_unlock(&task->task_mutex);

    if (pool->thread_count < pool->max_threads && pool->idle_count == 0) {
        pthread_create(&pool->threads[pool->thread_count++], NULL, worker_thread, pool);
    }

    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

int thread_pool_delete(struct thread_pool *pool)
{
    if (!pool) return TPOOL_ERR_INVALID_ARGUMENT;
    pthread_mutex_lock(&pool->mutex);
    if (pool->task_count > 0 || pool->running_count > 0) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_HAS_TASKS;
    }
    pool->shutdown = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    for (int i = 0; i < pool->thread_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }

    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->cond);
    free(pool->threads);
    free(pool);
    return 0;
}

int thread_task_new(struct thread_task **task, thread_task_f function, void *arg)
{
    if (!task || !function) return TPOOL_ERR_INVALID_ARGUMENT;
    struct thread_task *t = malloc(sizeof(*t));
    if (!t) return TPOOL_ERR_SYSTEM;
    pthread_mutex_init(&t->task_mutex, NULL);
    pthread_cond_init(&t->task_cond, NULL);
    t->function = function; t->arg = arg; t->next = NULL;
    t->state = TASK_CREATED; t->result = NULL;
    t->pushed = t->joined = t->detached = false;
    t->pool = NULL;
    *task = t;
    return 0;
}

bool thread_task_is_finished(const struct thread_task *task)
{
    if (!task) return false;
    pthread_mutex_lock((pthread_mutex_t*)&task->task_mutex);
    bool fin = task->state == TASK_FINISHED;
    pthread_mutex_unlock((pthread_mutex_t*)&task->task_mutex);
    return fin;
}

bool thread_task_is_running(const struct thread_task *task)
{
    if (!task) return false;
    pthread_mutex_lock((pthread_mutex_t*)&task->task_mutex);
    bool run = task->state == TASK_RUNNING;
    pthread_mutex_unlock((pthread_mutex_t*)&task->task_mutex);
    return run;
}

int thread_task_join(struct thread_task *task, void **result)
{
    if (!task) return TPOOL_ERR_INVALID_ARGUMENT;
    pthread_mutex_lock(&task->task_mutex);
    if (!task->pushed) { pthread_mutex_unlock(&task->task_mutex); return TPOOL_ERR_TASK_NOT_PUSHED; }
    while (task->state != TASK_FINISHED) {
        pthread_cond_wait(&task->task_cond, &task->task_mutex);
    }
    task->joined = true;
    if (result) *result = task->result;
    pthread_mutex_unlock(&task->task_mutex);
    return 0;
}

int thread_task_delete(struct thread_task *task)
{
    if (!task) return TPOOL_ERR_INVALID_ARGUMENT;
    pthread_mutex_lock(&task->task_mutex);
    bool can = !task->pushed || (task->state == TASK_FINISHED && task->joined);
    pthread_mutex_unlock(&task->task_mutex);
    if (!can) return TPOOL_ERR_TASK_IN_POOL;
    pthread_mutex_destroy(&task->task_mutex);
    pthread_cond_destroy(&task->task_cond);
    free(task);
    return 0;
}

#if NEED_TIMED_JOIN
int thread_task_timed_join(struct thread_task *task, double timeout, void **result)
{
    if (!task)
        return TPOOL_ERR_INVALID_ARGUMENT;
    
    pthread_mutex_lock(&task->task_mutex);
    if (!task->pushed) {
        pthread_mutex_unlock(&task->task_mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }
    
    if (timeout <= 0) {
        bool finished = (task->state == TASK_FINISHED);
        if (finished) {
            task->joined = true;
            if (result)
                *result = task->result;
        }
        pthread_mutex_unlock(&task->task_mutex);
        return finished ? 0 : TPOOL_ERR_TIMEOUT;
    }
    
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        pthread_mutex_unlock(&task->task_mutex);
        return TPOOL_ERR_SYSTEM;
    }
    
    time_t sec = (time_t)timeout;
    long nsec = (long)((timeout - sec) * 1e9);
    
    ts.tv_sec += sec;
    ts.tv_nsec += nsec;
    
    if (ts.tv_nsec >= 1000000000) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000;
    }
    
    int rc = 0;
    while (task->state != TASK_FINISHED && rc == 0) {
        rc = pthread_cond_timedwait(&task->task_cond, &task->task_mutex, &ts);
    }
    
    if (task->state != TASK_FINISHED) {
        pthread_mutex_unlock(&task->task_mutex);
        return (rc == ETIMEDOUT) ? TPOOL_ERR_TIMEOUT : TPOOL_ERR_SYSTEM;
    }
    
    task->joined = true;
    if (result)
        *result = task->result;
    
    pthread_mutex_unlock(&task->task_mutex);
    return 0;
}
#endif

#if NEED_DETACH
int thread_task_detach(struct thread_task *task)
{
    if (!task)
        return TPOOL_ERR_INVALID_ARGUMENT;
    
    pthread_mutex_lock(&task->task_mutex);
    if (!task->pushed) {
        pthread_mutex_unlock(&task->task_mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }
    
    if (task->state == TASK_FINISHED) {
        pthread_mutex_unlock(&task->task_mutex);
        pthread_mutex_destroy(&task->task_mutex);
        pthread_cond_destroy(&task->task_cond);
        free(task);
        return 0;
    } else {
        task->detached = true;
        pthread_mutex_unlock(&task->task_mutex);
        return 0;
    }
}
#endif