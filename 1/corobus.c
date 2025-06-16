#include "corobus.h"
#include "libcoro.h"
#include "rlist.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>

struct data_vector {
    unsigned *data;
    size_t size;
    size_t capacity;
};

static void data_vector_init(struct data_vector *v) {
    v->data = NULL;
    v->size = 0;
    v->capacity = 0;
}

static void data_vector_free(struct data_vector *v) {
    if (v->data) {
        free(v->data);
        v->data = NULL;
    }
    v->size = 0;
    v->capacity = 0;
}

static void data_vector_append_many(struct data_vector *v, const unsigned *data, size_t count) {
    if (v->size + count > v->capacity) {
        size_t newcap = v->capacity ? v->capacity * 2 : 4;
        if (newcap < v->size + count)
            newcap = v->size + count;
        
        unsigned *new_data = NULL;
        if (v->data == NULL) {
            new_data = malloc(newcap * sizeof(unsigned));
        } else {
            new_data = realloc(v->data, newcap * sizeof(unsigned));
        }
        
        if (!new_data) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_MEMORY);
            return;
        }
        
        v->data = new_data;
        v->capacity = newcap;
    }
    
    memcpy(v->data + v->size, data, count * sizeof(unsigned));
    v->size += count;
}

static void data_vector_append(struct data_vector *v, unsigned d) {
    data_vector_append_many(v, &d, 1);
}

static void data_vector_pop_first_many(struct data_vector *v, unsigned *out, size_t count) {
    assert(v->size >= count);
    memcpy(out, v->data, count * sizeof(unsigned));
    v->size -= count;
    
    if (v->size > 0)
        memmove(v->data, v->data + count, v->size * sizeof(unsigned));
}

static unsigned data_vector_pop_first(struct data_vector *v) {
    unsigned d;
    data_vector_pop_first_many(v, &d, 1);
    return d;
}

struct wakeup_entry {
    struct rlist base;
    struct coro *coro;
};

struct wakeup_queue {
    struct rlist coros;
};

static void wakeup_queue_init(struct wakeup_queue *q) {
    rlist_create(&q->coros);
}

static void wakeup_queue_suspend_this(struct wakeup_queue *q) {
    struct wakeup_entry entry;
    entry.coro = coro_this();
    rlist_add_tail_entry(&q->coros, &entry, base);
    coro_suspend();
    rlist_del_entry(&entry, base);
}

static void wakeup_queue_wakeup_one(struct wakeup_queue *q, enum coro_bus_error_code err) {
    if (rlist_empty(&q->coros))
        return;
    
    struct rlist *node = q->coros.next;
    rlist_del(node);
    struct wakeup_entry *e = (struct wakeup_entry *)node;
    coro_bus_errno_set(err);
    coro_wakeup(e->coro);
}

static void wakeup_queue_wakeup_all(struct wakeup_queue *q, enum coro_bus_error_code err) {
    while (!rlist_empty(&q->coros))
        wakeup_queue_wakeup_one(q, err);
}

struct coro_bus_channel {
    size_t size_limit;
    struct wakeup_queue send_queue;
    struct wakeup_queue recv_queue;
    struct data_vector data;
};

struct coro_bus {
    struct coro_bus_channel **channels;
    int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code coro_bus_errno(void) {
    return global_error;
}

void coro_bus_errno_set(enum coro_bus_error_code err) {
    global_error = err;
}

struct coro_bus *coro_bus_new(void) {
    struct coro_bus *bus = malloc(sizeof(*bus));
    if (!bus) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_MEMORY);
        return NULL;
    }
    bus->channels = NULL;
    bus->channel_count = 0;
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return bus;
}

void coro_bus_delete(struct coro_bus *bus) {
    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (!ch) continue;
        
        wakeup_queue_wakeup_all(&ch->send_queue, CORO_BUS_ERR_NO_CHANNEL);
        wakeup_queue_wakeup_all(&ch->recv_queue, CORO_BUS_ERR_NO_CHANNEL);
        
        data_vector_free(&ch->data);
        free(ch);
    }
    
    free(bus->channels);
    free(bus);
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
}

int coro_bus_channel_open(struct coro_bus *bus, size_t size_limit) {
    for (int i = 0; i < bus->channel_count; i++) {
        if (!bus->channels[i]) {
            struct coro_bus_channel *ch = malloc(sizeof(*ch));
            if (!ch) {
                coro_bus_errno_set(CORO_BUS_ERR_NO_MEMORY);
                return -1;
            }
            
            ch->size_limit = size_limit;
            data_vector_init(&ch->data);
            wakeup_queue_init(&ch->send_queue);
            wakeup_queue_init(&ch->recv_queue);
            bus->channels[i] = ch;
            coro_bus_errno_set(CORO_BUS_ERR_NONE);
            return i;
        }
    }
    
    int old = bus->channel_count;
    int ncount = old + 1;
    bus->channels = realloc(bus->channels, ncount * sizeof(*bus->channels));
    if (!bus->channels) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_MEMORY);
        return -1;
    }
    
    bus->channels[old] = NULL;
    bus->channel_count = ncount;
    
    struct coro_bus_channel *ch = malloc(sizeof(*ch));
    if (!ch) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_MEMORY);
        return -1;
    }
    
    ch->size_limit = size_limit;
    data_vector_init(&ch->data);
    wakeup_queue_init(&ch->send_queue);
    wakeup_queue_init(&ch->recv_queue);
    bus->channels[old] = ch;
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return old;
}

void coro_bus_channel_close(struct coro_bus *bus, int channel) {
    if (channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return;
    }
    
    struct coro_bus_channel *ch = bus->channels[channel];
    bus->channels[channel] = NULL;
    
    wakeup_queue_wakeup_all(&ch->send_queue, CORO_BUS_ERR_NO_CHANNEL);
    wakeup_queue_wakeup_all(&ch->recv_queue, CORO_BUS_ERR_NO_CHANNEL);
    
    coro_yield();
    
    data_vector_free(&ch->data);
    free(ch);
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
}

int coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data) {
    if (channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch->data.size < ch->size_limit) {
        data_vector_append(&ch->data, data);
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        wakeup_queue_wakeup_one(&ch->recv_queue, CORO_BUS_ERR_NONE);
        return 0;
    }
    
    coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
    return -1;
}

int coro_bus_send(struct coro_bus *bus, int channel, unsigned data) {
    while (1) {
        if (coro_bus_try_send(bus, channel, data) == 0)
            return 0;
        if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
            return -1;
        
        if (channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        
        struct coro_bus_channel *ch = bus->channels[channel];
        if (!ch) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        
        wakeup_queue_suspend_this(&ch->send_queue);
    }
}

int coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data) {
    if (channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch->data.size > 0) {
        *data = data_vector_pop_first(&ch->data);
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        wakeup_queue_wakeup_one(&ch->send_queue, CORO_BUS_ERR_NONE);
        return 0;
    }
    
    coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
    return -1;
}

int coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data) {
    while (1) {
        if (coro_bus_try_recv(bus, channel, data) == 0)
            return 0;
        if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
            return -1;
        
        if (channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        
        struct coro_bus_channel *ch = bus->channels[channel];
        if (!ch) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        
        wakeup_queue_suspend_this(&ch->recv_queue);
    }
}

#if NEED_BROADCAST
int coro_bus_try_broadcast(struct coro_bus *bus, unsigned data) {
    if (bus->channel_count == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (!ch) continue;
        if (ch->data.size >= ch->size_limit) {
            coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
            return -1;
        }
    }
    
    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (!ch) continue;
        data_vector_append(&ch->data, data);
        wakeup_queue_wakeup_one(&ch->recv_queue, CORO_BUS_ERR_NONE);
    }
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

int coro_bus_broadcast(struct coro_bus *bus, unsigned data) {
    while (1) {
        if (coro_bus_try_broadcast(bus, data) == 0)
            return 0;
        if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
            return -1;
        
        for (int i = 0; i < bus->channel_count; i++) {
            struct coro_bus_channel *ch = bus->channels[i];
            if (!ch) continue;
            if (ch->data.size < ch->size_limit) continue;
            wakeup_queue_suspend_this(&ch->send_queue);
            break;
        }
    }
}
#endif

#if NEED_BATCH
int coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count) {
    if (channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    struct coro_bus_channel *ch = bus->channels[channel];
    size_t space = ch->size_limit - ch->data.size;
    size_t n = count < space ? count : space;
    
    if (n == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    
    data_vector_append_many(&ch->data, data, n);
    for (size_t i = 0; i < n; i++)
        wakeup_queue_wakeup_one(&ch->recv_queue, CORO_BUS_ERR_NONE);
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return (int)n;
}

int coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count) {
    while (1) {
        int sent = coro_bus_try_send_v(bus, channel, data, count);
        if (sent != -1) return sent;
        if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) return -1;
        
        struct coro_bus_channel *ch = bus->channels[channel];
        if (!ch) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        
        wakeup_queue_suspend_this(&ch->send_queue);
    }
}

int coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity) {
    if (channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    struct coro_bus_channel *ch = bus->channels[channel];
    size_t avail = ch->data.size;
    size_t n = capacity < avail ? capacity : avail;
    
    if (n == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    
    data_vector_pop_first_many(&ch->data, data, n);
    for (size_t i = 0; i < n; i++)
        wakeup_queue_wakeup_one(&ch->send_queue, CORO_BUS_ERR_NONE);
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return (int)n;
}

int coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity) {
    while (1) {
        int got = coro_bus_try_recv_v(bus, channel, data, capacity);
        if (got > 0) return got;
        if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) return -1;
        
        struct coro_bus_channel *ch = bus->channels[channel];
        if (!ch) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        
        wakeup_queue_suspend_this(&ch->recv_queue);
    }
}
#endif