#pragma once

#include <stdbool.h>

/*
 * Если система не определила данные идентификаторы времени,
 * задаём их вручную для совместимости с тестами.
 */
#ifndef CLOCK_MONOTONIC
#define CLOCK_MONOTONIC 1
#endif

#ifndef CLOCK_REALTIME
#define CLOCK_REALTIME 0
#endif

/**
 * Определите, какие дополнительные функции хотите реализовать:
 * NEED_DETACH и NEED_TIMED_JOIN.
 */
#define NEED_DETACH 1
#define NEED_TIMED_JOIN 1

struct thread_pool;
struct thread_task;

typedef void *(*thread_task_f)(void *);

enum {
    TPOOL_MAX_THREADS = 20,
    TPOOL_MAX_TASKS = 100000,
};

enum thread_poool_errcode {
    TPOOL_ERR_INVALID_ARGUMENT = 1,
    TPOOL_ERR_TOO_MANY_TASKS,
    TPOOL_ERR_HAS_TASKS,
    TPOOL_ERR_TASK_NOT_PUSHED,
    TPOOL_ERR_TASK_IN_POOL,
    TPOOL_ERR_NOT_IMPLEMENTED,
    TPOOL_ERR_TIMEOUT,
	TPOOL_ERR_NOT_ENOUGH_MEMORY,
	TPOOL_ERR_SYSTEM
};

/** API пула потоков. */
int thread_pool_new(int max_thread_count, struct thread_pool **pool);
int thread_pool_thread_count(const struct thread_pool *pool);
int thread_pool_delete(struct thread_pool *pool);
int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task);

/** API задач пула потоков. */
int thread_task_new(struct thread_task **task, thread_task_f function, void *arg);
bool thread_task_is_finished(const struct thread_task *task);
bool thread_task_is_running(const struct thread_task *task);
int thread_task_join(struct thread_task *task, void **result);

#if NEED_TIMED_JOIN
int thread_task_timed_join(struct thread_task *task, double timeout, void **result);
#endif

int thread_task_delete(struct thread_task *task);

#if NEED_DETACH
int thread_task_detach(struct thread_task *task);
#endif
