#include "corobus.h"
#include "libcoro.h"
#include "rlist.h"
#include <assert.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

/* ===== data_vector ===== */
struct data_vector {
    unsigned *data;
    size_t size;
    size_t capacity;
};

static void data_vector_init(struct data_vector *v) {
    v->data = NULL;
    v->size = v->capacity = 0;
}

static void data_vector_free(struct data_vector *v) {
    free(v->data);
}

static void data_vector_append_many(struct data_vector *v, const unsigned *src, size_t n) {
    if (v->size + n > v->capacity) {
        if (v->capacity == 0) v->capacity = 4;
        else v->capacity *= 2;
        if (v->capacity < v->size + n) v->capacity = v->size + n;
        v->data = realloc(v->data, v->capacity * sizeof *v->data);
    }
    memcpy(v->data + v->size, src, n * sizeof *src);
    v->size += n;
}

static void data_vector_append(struct data_vector *v, unsigned x) {
    data_vector_append_many(v, &x, 1);
}

static void data_vector_pop_first_many(struct data_vector *v, unsigned *dst, size_t n) {
    assert(n <= v->size);
    memcpy(dst, v->data, n * sizeof *dst);
    v->size -= n;
    memmove(v->data, v->data + n, v->size * sizeof *v->data);
}

static unsigned data_vector_pop_first(struct data_vector *v) {
    unsigned x;
    data_vector_pop_first_many(v, &x, 1);
    return x;
}

static size_t data_vector_pop_many(struct data_vector *v, unsigned *dst, size_t cap) {
    size_t t = v->size < cap ? v->size : cap;
    memcpy(dst, v->data, t * sizeof *dst);
    v->size -= t;
    memmove(v->data, v->data + t, v->size * sizeof *v->data);
    return t;
}

/* ===== wakeup queue ===== */
struct wakeup_entry { struct rlist base; struct coro *coro; };
struct wakeup_queue  { struct rlist coros;       };

static void wakeup_queue_init(struct wakeup_queue *q) {
    rlist_create(&q->coros);
}

static void wakeup_queue_suspend(struct wakeup_queue *q) {
    struct wakeup_entry entry;
    entry.coro = coro_this();
    rlist_add_tail_entry(&q->coros, &entry, base);
    coro_suspend();
    rlist_del_entry(&entry, base);
}

static void wakeup_queue_wakeup_all(struct wakeup_queue *q) {
    if (!rlist_empty(&q->coros)) {
        struct wakeup_entry *e = rlist_first_entry(&q->coros, struct wakeup_entry, base);
        coro_wakeup(e->coro);
    }
}

/* ===== coro bus ===== */
struct coro_bus_channel {
    size_t size_limit;
    struct data_vector buf;
    struct wakeup_queue send_q;
    struct wakeup_queue recv_q;
    int closed;
};

struct coro_bus {
    struct coro_bus_channel **channels;
    int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code coro_bus_errno(void) {
    return global_error;
}

void coro_bus_errno_set(enum coro_bus_error_code e) {
    global_error = e;
}

struct coro_bus *coro_bus_new(void) {
    struct coro_bus *bus = malloc(sizeof *bus);
    bus->channels = NULL;
    bus->channel_count = 0;
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return bus;
}

void coro_bus_delete(struct coro_bus *bus) {
    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *c = bus->channels[i];
        if (c) {
            data_vector_free(&c->buf);
            free(c);
        }
    }
    free(bus->channels);
    free(bus);
}

static int channel_get(struct coro_bus *bus, int idx, struct coro_bus_channel **out) {
    if (idx < 0 || idx >= bus->channel_count || !bus->channels[idx]) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    *out = bus->channels[idx];
    if ((*out)->closed) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    return 0;
}

int coro_bus_channel_open(struct coro_bus *bus, size_t size_limit) {
    struct coro_bus_channel *c = malloc(sizeof *c);
    c->size_limit = size_limit;
    data_vector_init(&c->buf);
    wakeup_queue_init(&c->send_q);
    wakeup_queue_init(&c->recv_q);
    c->closed = 0;

    int idx = -1;
    for (int i = 0; i < bus->channel_count; i++) {
        if (!bus->channels[i]) { idx = i; break; }
    }
    if (idx < 0) {
        idx = bus->channel_count;
        bus->channels = realloc(bus->channels, (bus->channel_count+1)*sizeof *bus->channels);
        bus->channel_count++;
    }
    bus->channels[idx] = c;
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return idx;
}

void coro_bus_channel_close(struct coro_bus *bus, int idx) {
    struct coro_bus_channel *c;
    if (channel_get(bus, idx, &c) < 0) return;
    c->closed = 1;
    wakeup_queue_wakeup_all(&c->send_q);
    wakeup_queue_wakeup_all(&c->recv_q);
    data_vector_free(&c->buf);
    free(c);
    bus->channels[idx] = NULL;
}

int coro_bus_send(struct coro_bus *bus, int idx, unsigned data) {
    struct coro_bus_channel *c;
    if (channel_get(bus, idx, &c) < 0) return -1;
    while (c->buf.size >= c->size_limit) {
        wakeup_queue_suspend(&c->send_q);
        if (c->closed) { coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL); return -1; }
    }
    data_vector_append(&c->buf, data);
    wakeup_queue_wakeup_all(&c->recv_q);
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

int coro_bus_try_send(struct coro_bus *bus, int idx, unsigned data) {
    struct coro_bus_channel *c;
    if (channel_get(bus, idx, &c) < 0) return -1;
    if (c->buf.size >= c->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    data_vector_append(&c->buf, data);
    wakeup_queue_wakeup_all(&c->recv_q);
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

int coro_bus_recv(struct coro_bus *bus, int idx, unsigned *out) {
    struct coro_bus_channel *c;
    if (channel_get(bus, idx, &c) < 0) return -1;
    while (c->buf.size == 0) {
        wakeup_queue_suspend(&c->recv_q);
        if (c->closed) { coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL); return -1; }
    }
    *out = data_vector_pop_first(&c->buf);
    wakeup_queue_wakeup_all(&c->send_q);
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

int coro_bus_try_recv(struct coro_bus *bus, int idx, unsigned *out) {
    struct coro_bus_channel *c;
    if (channel_get(bus, idx, &c) < 0) return -1;
    if (c->buf.size == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    *out = data_vector_pop_first(&c->buf);
    wakeup_queue_wakeup_all(&c->send_q);
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

#if NEED_BROADCAST
int coro_bus_broadcast(struct coro_bus *bus, unsigned data) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    if (bus->channel_count == 0) { coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL); return -1; }
    while (true) {
        bool blocked = false;
        for (int i = 0; i < bus->channel_count; i++) {
            struct coro_bus_channel *c = bus->channels[i];
            if (c && !c->closed && c->buf.size >= c->size_limit) {
                blocked = true;
                wakeup_queue_suspend(&c->send_q);
                break;
            }
        }
        if (!blocked) break;
    }
    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *c = bus->channels[i];
        if (c && !c->closed) {
            data_vector_append(&c->buf, data);
            wakeup_queue_wakeup_all(&c->recv_q);
        }
    }
    return 0;
}

int coro_bus_try_broadcast(struct coro_bus *bus, unsigned data) {
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    if (bus->channel_count == 0) { coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL); return -1; }
    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *c = bus->channels[i];
        if (c && !c->closed && c->buf.size >= c->size_limit) {
            coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
            return -1;
        }
    }
    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *c = bus->channels[i];
        if (c && !c->closed) {
            data_vector_append(&c->buf, data);
            wakeup_queue_wakeup_all(&c->recv_q);
        }
    }
    return 0;
}
#endif

#if NEED_BATCH
int coro_bus_send_v(struct coro_bus *bus, int idx, const unsigned *data, unsigned cnt) {
    int sent = 0;
    while (sent == 0) {
        sent = coro_bus_try_send_v(bus, idx, data, cnt);
        if (sent <= 0) {
            if (global_error == CORO_BUS_ERR_WOULD_BLOCK) continue;
            return -1;
        }
    }
    return sent;
}

int coro_bus_try_send_v(struct coro_bus *bus, int idx, const unsigned *data, unsigned cnt) {
    struct coro_bus_channel *c;
    if (channel_get(bus, idx, &c) < 0) return -1;
    size_t avail = c->size_limit - c->buf.size;
    if (avail == 0) { coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK); return -1; }
    size_t tosend = avail < cnt ? avail : cnt;
    data_vector_append_many(&c->buf, data, tosend);
    wakeup_queue_wakeup_all(&c->recv_q);
    return (int)tosend;
}

int coro_bus_recv_v(struct coro_bus *bus, int idx, unsigned *dst, unsigned cap) {
    int recv = 0;
    while (recv == 0) {
        recv = coro_bus_try_recv_v(bus, idx, dst, cap);
        if (recv <= 0) {
            if (global_error == CORO_BUS_ERR_WOULD_BLOCK) continue;
            return -1;
        }
    }
    return recv;
}

int coro_bus_try_recv_v(struct coro_bus *bus, int idx, unsigned *dst, unsigned cap) {
    struct coro_bus_channel *c;
    if (channel_get(bus, idx, &c) < 0) return -1;
    if (c->buf.size == 0) { coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK); return -1; }
    size_t recv = c->buf.size < cap ? c->buf.size : cap;
    data_vector_pop_many(&c->buf, dst, recv);
    wakeup_queue_wakeup_all(&c->send_q);
    return (int)recv;
}
#endif
