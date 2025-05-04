#include "thread_pool.h"
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h>
#include <errno.h>

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
    struct thread_pool *pool;
};

struct thread_pool {
    pthread_mutex_t tasks_mutex;
    pthread_cond_t tasks_cv;
    int max_threads;
    int num_threads;
    int active_threads;
    int queued_tasks_count;
    struct thread_task *task_queue_head;
    struct thread_task *task_queue_tail;
    bool is_shutdown;
    pthread_t *threads;
};

static void *worker(void *arg);

int thread_pool_new(int max_thread_count, struct thread_pool **pool) {
    if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS) {
        return TPOOL_ERR_INVALID_ARGUMENT;
    }

    *pool = malloc(sizeof(struct thread_pool));
    if (*pool == NULL) {
        return TPOOL_ERR_SYSTEM;
    }

    (*pool)->max_threads = max_thread_count;
    (*pool)->num_threads = 0;
    (*pool)->active_threads = 0;
    (*pool)->queued_tasks_count = 0;
    (*pool)->task_queue_head = NULL;
    (*pool)->task_queue_tail = NULL;
    (*pool)->is_shutdown = false;
    (*pool)->threads = malloc(max_thread_count * sizeof(pthread_t));
    if ((*pool)->threads == NULL) {
        free(*pool);
        return TPOOL_ERR_SYSTEM;
    }

    pthread_mutex_init(&(*pool)->tasks_mutex, NULL);
    pthread_cond_init(&(*pool)->tasks_cv, NULL);

    return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool) {
    return pool->num_threads;
}

int thread_pool_delete(struct thread_pool *pool) {
    pthread_mutex_lock(&pool->tasks_mutex);
    if (pool->task_queue_head != NULL || pool->queued_tasks_count > 0) {
        pthread_mutex_unlock(&pool->tasks_mutex);
        return TPOOL_ERR_HAS_TASKS;
    }

    pool->is_shutdown = true;
    pthread_cond_broadcast(&pool->tasks_cv);
    pthread_mutex_unlock(&pool->tasks_mutex);

    for (int i = 0; i < pool->num_threads; i++) {
        pthread_join(pool->threads[i], NULL);
    }

    pthread_mutex_destroy(&pool->tasks_mutex);
    pthread_cond_destroy(&pool->tasks_cv);
    free(pool->threads);
    free(pool);

    return 0;
}

int thread_task_new(struct thread_task **task, thread_task_f function, void *arg) {
    *task = malloc(sizeof(struct thread_task));
    if (*task == NULL) {
        return TPOOL_ERR_SYSTEM;
    }

    (*task)->func = function;
    (*task)->arg = arg;
    (*task)->result = NULL;
    (*task)->is_finished = false;
    (*task)->is_pushed = false;
    (*task)->is_detached = false;
    (*task)->next = NULL;
    (*task)->pool = NULL;

    pthread_mutex_init(&(*task)->mutex, NULL);
    pthread_cond_init(&(*task)->cond, NULL);

    return 0;
}

int thread_task_delete(struct thread_task *task) {
    if (task == NULL) {
        return TPOOL_ERR_INVALID_ARGUMENT;
    }

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
    if (pool == NULL || task == NULL) {
        return TPOOL_ERR_INVALID_ARGUMENT;
    }

    pthread_mutex_lock(&task->mutex);
    if (task->is_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_IN_POOL;
    }

    task->is_finished = false;
    task->is_pushed = true;
    task->is_detached = false;
    task->pool = pool;
    pthread_mutex_unlock(&task->mutex);

    pthread_mutex_lock(&pool->tasks_mutex);
    if (pool->queued_tasks_count >= TPOOL_MAX_TASKS) {
        // Откат is_pushed, если очередь переполнена
        pthread_mutex_lock(&task->mutex);
        task->is_pushed = false;
        pthread_mutex_unlock(&task->mutex);
        
        pthread_mutex_unlock(&pool->tasks_mutex);
        return TPOOL_ERR_TOO_MANY_TASKS;
    }

    if (pool->task_queue_tail == NULL) {
        pool->task_queue_head = task;
        pool->task_queue_tail = task;
    } else {
        pool->task_queue_tail->next = task;
        pool->task_queue_tail = task;
    }
    task->next = NULL;
    pool->queued_tasks_count++;

    if (pool->num_threads < pool->max_threads && 
        pool->active_threads == pool->num_threads) {
        int err = pthread_create(&pool->threads[pool->num_threads], NULL, worker, pool);
        if (err == 0) {
            pool->num_threads++;
        }
    }

    pthread_cond_signal(&pool->tasks_cv);
    pthread_mutex_unlock(&pool->tasks_mutex);

    return 0;
}

static void *worker(void *arg) {
    struct thread_pool *pool = arg;

    while (1) {
        pthread_mutex_lock(&pool->tasks_mutex);
        while (pool->task_queue_head == NULL && !pool->is_shutdown) {
            pthread_cond_wait(&pool->tasks_cv, &pool->tasks_mutex);
        }

        if (pool->is_shutdown) {
            pthread_mutex_unlock(&pool->tasks_mutex);
            break;
        }

        struct thread_task *task = pool->task_queue_head;
        pool->task_queue_head = task->next;
        if (pool->task_queue_head == NULL) {
            pool->task_queue_tail = NULL;
        }
        pool->queued_tasks_count--;
        pool->active_threads++;
        pthread_mutex_unlock(&pool->tasks_mutex);

        void *result = task->func(task->arg);

        pthread_mutex_lock(&task->mutex);
        task->result = result;
        task->is_finished = true;
        pthread_cond_broadcast(&task->cond);
        bool is_detached = task->is_detached;
        pthread_mutex_unlock(&task->mutex);

        if (is_detached) {
            thread_task_delete(task);
        }

        pthread_mutex_lock(&pool->tasks_mutex);
        pool->active_threads--;
        pthread_mutex_unlock(&pool->tasks_mutex);
    }

    return NULL;
}

int thread_task_join(struct thread_task *task, void **result) {
    if (task == NULL) {
        return TPOOL_ERR_INVALID_ARGUMENT;
    }

    pthread_mutex_lock(&task->mutex);
    if (!task->is_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    while (!task->is_finished) {
        pthread_cond_wait(&task->cond, &task->mutex);
    }

    if (result != NULL) {
        *result = task->result;
    }

    pthread_mutex_unlock(&task->mutex);
    return 0;
}

#if NEED_TIMED_JOIN
int thread_task_timed_join(struct thread_task *task, double timeout, void **result) {
    if (task == NULL) {
        return TPOOL_ERR_INVALID_ARGUMENT;
    }

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    ts.tv_sec += (time_t)timeout;
    ts.tv_nsec += (long)((timeout - (time_t)timeout) * 1e9);

    if (ts.tv_nsec >= 1e9) {
        ts.tv_sec += 1;
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

    if (result != NULL) {
        *result = task->result;
    }

    pthread_mutex_unlock(&task->mutex);
    return 0;
}
#endif

#if NEED_DETACH
int thread_task_detach(struct thread_task *task) {
    if (task == NULL) {
        return TPOOL_ERR_INVALID_ARGUMENT;
    }

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