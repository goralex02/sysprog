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

static void
data_vector_append_many(struct data_vector *vector,
                         const unsigned *data, size_t count)
{
    if (vector->size + count > vector->capacity) {
        if (vector->capacity == 0)
            vector->capacity = 4;
        else
            vector->capacity *= 2;
        if (vector->capacity < vector->size + count)
            vector->capacity = vector->size + count;
        vector->data = realloc(vector->data, sizeof(vector->data[0]) * vector->capacity);
    }
    memcpy(&vector->data[vector->size], data, sizeof(data[0]) * count);
    vector->size += count;
}

static void
data_vector_append(struct data_vector *vector, unsigned data)
{
    data_vector_append_many(vector, &data, 1);
}

static void
data_vector_pop_first_many(struct data_vector *vector, unsigned *data, size_t count)
{
    assert(count <= vector->size);
    memcpy(data, vector->data, sizeof(data[0]) * count);
    vector->size -= count;
    memmove(vector->data, &vector->data[count], vector->size * sizeof(vector->data[0]));
}

static unsigned
data_vector_pop_first(struct data_vector *vector)
{
    unsigned data = 0;
    data_vector_pop_first_many(vector, &data, 1);
    return data;
}

struct wakeup_entry {
    struct rlist base;
    struct coro *coro;
};

struct wakeup_queue {
    struct rlist coros;
};

static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
    struct wakeup_entry entry;
    entry.coro = coro_this();
    rlist_add_tail_entry(&queue->coros, &entry, base);
    coro_suspend();
    rlist_del_entry(&entry, base);
}

static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
    if (rlist_empty(&queue->coros))
        return;
    struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
                                                   struct wakeup_entry, base);
    coro_wakeup(entry->coro);
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

enum coro_bus_error_code
coro_bus_errno(void)
{
    return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
    global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
    struct coro_bus *bus = malloc(sizeof(*bus));
    bus->channels = NULL;
    bus->channel_count = 0;
    return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
    for (int i = 0; i < bus->channel_count; ++i) {
        free(bus->channels[i]->data.data);
        free(bus->channels[i]);
    }
    free(bus->channels);
    free(bus);
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
    struct coro_bus_channel *channel = malloc(sizeof(*channel));
    channel->size_limit = size_limit;
    rlist_create(&channel->send_queue.coros);
    rlist_create(&channel->recv_queue.coros);
    channel->data.data = NULL;
    channel->data.size = 0;
    channel->data.capacity = 0;

    bus->channels = realloc(bus->channels, sizeof(*bus->channels) * (bus->channel_count + 1));
    bus->channels[bus->channel_count] = channel;
    return bus->channel_count++;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    while (!rlist_empty(&ch->send_queue.coros)) {
        struct wakeup_entry *entry = rlist_first_entry(&ch->send_queue.coros,
                                                       struct wakeup_entry, base);
        coro_wakeup(entry->coro);
    }
    while (!rlist_empty(&ch->recv_queue.coros)) {
        struct wakeup_entry *entry = rlist_first_entry(&ch->recv_queue.coros,
                                                       struct wakeup_entry, base);
        coro_wakeup(entry->coro);
    }
    free(ch->data.data);
    free(ch);

    // Удаляем канал из массива
    memmove(&bus->channels[channel], &bus->channels[channel + 1],
            sizeof(*bus->channels) * (bus->channel_count - channel - 1));
    --bus->channel_count;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    while (true) {
        if (ch->data.size < ch->size_limit) {
            data_vector_append(&ch->data, data);
            wakeup_queue_wakeup_first(&ch->recv_queue);
            return 0;
        }
        wakeup_queue_suspend_this(&ch->send_queue);
    }
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch->data.size >= ch->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    data_vector_append(&ch->data, data);
    wakeup_queue_wakeup_first(&ch->recv_queue);
    return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    while (true) {
        if (ch->data.size > 0) {
            *data = data_vector_pop_first(&ch->data);
            wakeup_queue_wakeup_first(&ch->send_queue);
            return 0;
        }
        wakeup_queue_suspend_this(&ch->recv_queue);
    }
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch->data.size == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    *data = data_vector_pop_first(&ch->data);
    wakeup_queue_wakeup_first(&ch->send_queue);
    return 0;
}

#if NEED_BROADCAST
int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
    if (bus->channel_count == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    bool all_channels_full = true;
    for (int i = 0; i < bus->channel_count; ++i) {
        if (bus->channels[i]->data.size < bus->channels[i]->size_limit) {
            all_channels_full = false;
            break;
        }
    }

    while (all_channels_full) {
        for (int i = 0; i < bus->channel_count; ++i) {
            wakeup_queue_suspend_this(&bus->channels[i]->send_queue);
        }
        all_channels_full = true;
        for (int i = 0; i < bus->channel_count; ++i) {
            if (bus->channels[i]->data.size < bus->channels[i]->size_limit) {
                all_channels_full = false;
                break;
            }
        }
    }

    for (int i = 0; i < bus->channel_count; ++i) {
        data_vector_append(&bus->channels[i]->data, data);
        wakeup_queue_wakeup_first(&bus->channels[i]->recv_queue);
    }
    return 0;
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
    if (bus->channel_count == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    for (int i = 0; i < bus->channel_count; ++i) {
        if (bus->channels[i]->data.size >= bus->channels[i]->size_limit) {
            coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
            return -1;
        }
    }

    for (int i = 0; i < bus->channel_count; ++i) {
        data_vector_append(&bus->channels[i]->data, data);
        wakeup_queue_wakeup_first(&bus->channels[i]->recv_queue);
    }
    return 0;
}
#endif

#if NEED_BATCH
int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    unsigned sent = 0;

    while (sent < count) {
        if (ch->data.size < ch->size_limit) {
            size_t to_send = ch->size_limit - ch->data.size;
            if (to_send > count - sent)
                to_send = count - sent;
            data_vector_append_many(&ch->data, data + sent, to_send);
            sent += to_send;
            wakeup_queue_wakeup_first(&ch->recv_queue);
        } else {
            wakeup_queue_suspend_this(&ch->send_queue);
        }
    }
    return sent;
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch->data.size + count > ch->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    data_vector_append_many(&ch->data, data, count);
    wakeup_queue_wakeup_first(&ch->recv_queue);
    return count;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    unsigned received = 0;

    while (received < capacity) {
        if (ch->data.size > 0) {
            size_t to_recv = ch->data.size;
            if (to_recv > capacity - received)
                to_recv = capacity - received;
            data_vector_pop_first_many(&ch->data, data + received, to_recv);
            received += to_recv;
            wakeup_queue_wakeup_first(&ch->send_queue);
        } else {
            wakeup_queue_suspend_this(&ch->recv_queue);
        }
    }
    return received;
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch->data.size == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    size_t to_recv = ch->data.size;
    if (to_recv > capacity)
        to_recv = capacity;
    data_vector_pop_first_many(&ch->data, data, to_recv);
    wakeup_queue_wakeup_first(&ch->send_queue);
    return to_recv;
}
#endif