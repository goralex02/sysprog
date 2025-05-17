#include "corobus.h"
#include "libcoro.h"
#include "rlist.h"
#include <assert.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

struct data_vector {
    unsigned *data;
    size_t size;
    size_t capacity;
};
static void data_vector_init(struct data_vector *v) { v->data = NULL; v->size = v->capacity = 0; }
static void data_vector_free(struct data_vector *v) { free(v->data); }
static void data_vector_append_many(struct data_vector *v, const unsigned *src, size_t n) {
    assert(v->size + n <= v->capacity);
    memcpy(v->data + v->size, src, n * sizeof *src);
    v->size += n;
}
static void data_vector_append(struct data_vector *v, unsigned x) { data_vector_append_many(v, &x, 1); }
static unsigned data_vector_pop_front(struct data_vector *v) {
    assert(v->size > 0);
    unsigned x = v->data[0];
    memmove(v->data, v->data + 1, (v->size - 1) * sizeof *v->data);
    v->size--;
    return x;
}
static size_t data_vector_pop_many(struct data_vector *v, unsigned *dst, size_t n) {
    size_t take = n < v->size ? n : v->size;
    memcpy(dst, v->data, take * sizeof *dst);
    memmove(v->data, v->data + take, (v->size - take) * sizeof *v->data);
    v->size -= take;
    return take;
}

struct wakeup_entry { struct rlist base; struct coro *coro; };
struct wakeup_queue { struct rlist list; };

static void wakeup_queue_init(struct wakeup_queue *q) {
    rlist_create(&q->list);
}

static void wakeup_queue_suspend(struct wakeup_queue *q) {
    struct wakeup_entry entry;
    entry.coro = coro_this();
    rlist_create(&entry.base);
    rlist_add_tail_entry(&q->list, &entry, base);
    coro_suspend();
    rlist_del_entry(&entry, base);
}

static void wakeup_queue_wakeup_all(struct wakeup_queue *q) {
    while (!rlist_empty(&q->list)) {
        struct wakeup_entry *e = rlist_first_entry(&q->list, struct wakeup_entry, base);
        rlist_del_entry(e, base);
        coro_wakeup(e->coro);
    }
}

struct coro_bus_channel {
    size_t capacity;
    struct data_vector buf;
    struct wakeup_queue send_q, recv_q;
    int closed;
};
struct coro_bus { struct coro_bus_channel **ch; int nch; };
static enum coro_bus_error_code err;
enum coro_bus_error_code coro_bus_errno(void) { return err; }
void coro_bus_errno_set(enum coro_bus_error_code e) { err = e; }

struct coro_bus *coro_bus_new(void) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus *b = malloc(sizeof *b);
    b->ch = NULL;
    b->nch = 0;
    return b;
}
void coro_bus_delete(struct coro_bus *b) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    for (int i = 0; i < b->nch; i++) {
        if (b->ch[i]) {
            data_vector_free(&b->ch[i]->buf);
            free(b->ch[i]);
        }
    }
    free(b->ch);
    free(b);
}

int coro_bus_channel_open(struct coro_bus *b, size_t cap) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c = malloc(sizeof *c);
    c->capacity = cap;
    data_vector_init(&c->buf);
    c->buf.data = malloc(cap * sizeof *c->buf.data);
    c->buf.capacity = cap;
    c->buf.size = 0;
    wakeup_queue_init(&c->send_q);
    wakeup_queue_init(&c->recv_q);
    c->closed = 0;
    int idx = -1;
    for (int i = 0; i < b->nch; i++) if (!b->ch[i]) { idx = i; break; }
    if (idx < 0) {
        idx = b->nch;
        b->ch = realloc(b->ch, (b->nch + 1) * sizeof *b->ch);
        b->nch++;
    }
    b->ch[idx] = c;
    return idx;
}
void coro_bus_channel_close(struct coro_bus *b, int idx) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    if (idx < 0 || idx >= b->nch || !b->ch[idx]) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return;
    }
    struct coro_bus_channel *c = b->ch[idx];
    c->closed = 1;
    wakeup_queue_wakeup_all(&c->send_q);
    wakeup_queue_wakeup_all(&c->recv_q);
    data_vector_free(&c->buf);
    free(c);
    b->ch[idx] = NULL;
}

static int channel_get(struct coro_bus *b, int idx, struct coro_bus_channel **out) {
    if (idx < 0 || idx >= b->nch || !b->ch[idx]) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    *out = b->ch[idx];
    if ((*out)->closed) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    return 0;
}

int coro_bus_send(struct coro_bus *b, int idx, unsigned x) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c;
    if (channel_get(b, idx, &c) < 0) return -1;
    while (c->buf.size >= c->capacity) {
        wakeup_queue_suspend(&c->send_q);
        if (c->closed) { coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL); return -1; }
    }
    data_vector_append(&c->buf, x);
    wakeup_queue_wakeup_all(&c->recv_q);
    return 0;
}
int coro_bus_try_send(struct coro_bus *b, int idx, unsigned x) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c;
    if (channel_get(b, idx, &c) < 0) return -1;
    if (c->buf.size >= c->capacity) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    data_vector_append(&c->buf, x);
    wakeup_queue_wakeup_all(&c->recv_q);
    return 0;
}
int coro_bus_recv(struct coro_bus *b, int idx, unsigned *out) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c;
    if (channel_get(b, idx, &c) < 0) return -1;
    while (c->buf.size == 0) {
        wakeup_queue_suspend(&c->recv_q);
        if (c->closed) { coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL); return -1; }
    }
    *out = data_vector_pop_front(&c->buf);
    wakeup_queue_wakeup_all(&c->send_q);
    return 0;
}
int coro_bus_try_recv(struct coro_bus *b, int idx, unsigned *out) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c;
    if (channel_get(b, idx, &c) < 0) return -1;
    if (c->buf.size == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    *out = data_vector_pop_front(&c->buf);
    wakeup_queue_wakeup_all(&c->send_q);
    return 0;
}

#if NEED_BROADCAST
int coro_bus_broadcast(struct coro_bus *b, unsigned x) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    if (b->nch == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    while (1) {
        int any = 0;
        int all_free = 1;
        for (int i = 0; i < b->nch; i++) {
            struct coro_bus_channel *c = b->ch[i];
            if (c && !c->closed) {
                any = 1;
                if (c->buf.size >= c->capacity) {
                    all_free = 0;
                    wakeup_queue_suspend(&c->send_q);
                    break;
                }
            }
        }
        if (!any) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        if (all_free) break;
    }
    for (int i = 0; i < b->nch; i++) {
        struct coro_bus_channel *c = b->ch[i];
        if (c && !c->closed) {
            data_vector_append(&c->buf, x);
            wakeup_queue_wakeup_all(&c->recv_q);
        }
    }
    return 0;
}

int coro_bus_try_broadcast(struct coro_bus *b, unsigned x) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    if (b->nch == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    int any = 0;
    for (int i = 0; i < b->nch; i++) {
        struct coro_bus_channel *c = b->ch[i];
        if (c && !c->closed) {
            any = 1;
            if (c->buf.size >= c->capacity) {
                coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
                return -1;
            }
        }
    }
    if (!any) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    for (int i = 0; i < b->nch; i++) {
        struct coro_bus_channel *c = b->ch[i];
        if (c && !c->closed) {
            data_vector_append(&c->buf, x);
            wakeup_queue_wakeup_all(&c->recv_q);
        }
    }
    return 0;
}
#endif

#if NEED_BATCH
int coro_bus_send_v(struct coro_bus *b, int idx, const unsigned *data, unsigned cnt) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c;
    if (channel_get(b, idx, &c) < 0) return -1;
    while (c->buf.size >= c->capacity) {
        wakeup_queue_suspend(&c->send_q);
        if (c->closed) { coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL); return -1; }
    }
    size_t avail = c->capacity - c->buf.size;
    size_t sent = avail < cnt ? avail : cnt;
    data_vector_append_many(&c->buf, data, sent);
    wakeup_queue_wakeup_all(&c->recv_q);
    return (int)sent;
}
int coro_bus_try_send_v(struct coro_bus *b, int idx, const unsigned *data, unsigned cnt) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c;
    if (channel_get(b, idx, &c) < 0) return -1;
    size_t avail = c->capacity - c->buf.size;
    if (avail == 0) { coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK); return -1; }
    size_t sent = avail < cnt ? avail : cnt;
    data_vector_append_many(&c->buf, data, sent);
    wakeup_queue_wakeup_all(&c->recv_q);
    return (int)sent;
}
int coro_bus_recv_v(struct coro_bus *b, int idx, unsigned *data, unsigned cap) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c;
    if (channel_get(b, idx, &c) < 0) return -1;
    while (c->buf.size == 0) {
        wakeup_queue_suspend(&c->recv_q);
        if (c->closed) { coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL); return -1; }
    }
    size_t torecv = c->buf.size < cap ? c->buf.size : cap;
    data_vector_pop_many(&c->buf, data, torecv);
    wakeup_queue_wakeup_all(&c->send_q);
    return (int)torecv;
}
int coro_bus_try_recv_v(struct coro_bus *b, int idx, unsigned *data, unsigned cap) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    struct coro_bus_channel *c;
    if (channel_get(b, idx, &c) < 0) return -1;
    if (c->buf.size == 0) { coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK); return -1; }
    size_t torecv = c->buf.size < cap ? c->buf.size : cap;
    data_vector_pop_many(&c->buf, data, torecv);
    wakeup_queue_wakeup_all(&c->send_q);
    return (int)torecv;
}
#endif
