#include "corobus.h"
#include "libcoro.h"
#include "rlist.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#ifndef rlist_for_each_safe
#define rlist_for_each_safe(pos, n, head) \
    for ((pos) = (head)->next, (n) = (pos)->next; (pos) != (head); (pos) = (n), (n) = (pos)->next)
#endif

/* ====== data_vector ====== */
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

/* ====== wakeup_queue ====== */
struct wakeup_entry {
    struct rlist base;
    struct coro *coro;
};

struct wakeup_queue {
    struct rlist coros;
};

/* Функция для пробуждения всех ожидающих корутин */
static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
    struct rlist *pos, *n;
    rlist_for_each_safe(pos, n, &queue->coros) {
         struct wakeup_entry *entry = rlist_entry(pos, struct wakeup_entry, base);
         coro_wakeup(entry->coro);
         /* Не удаляем элемент – при возобновлении корутины она сама вызовет rlist_del_entry */
    }
}

static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
    struct wakeup_entry entry;
    entry.coro = coro_this();
    rlist_add_tail_entry(&queue->coros, &entry, base);
    coro_suspend();
    /* При возобновлении корутины удаляем свою запись, если она ещё в списке */
    if (!rlist_empty(&entry.base))
        rlist_del_entry(&entry, base);
}

/* ====== Структуры для шины ====== */
struct coro_bus_channel {
    size_t size_limit;
    struct wakeup_queue send_queue;
    struct wakeup_queue recv_queue;
    struct data_vector data;
    int closed;  // 0 - открыт, 1 - закрыт
};

struct coro_bus {
    struct coro_bus_channel **channels;
    int channel_count; /* размер массива (количество слотов) */
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

/* При удалении шины освобождаем все оставшиеся каналы */
void
coro_bus_delete(struct coro_bus *bus)
{
    for (int i = 0; i < bus->channel_count; ++i) {
        if (bus->channels[i] != NULL) {
            free(bus->channels[i]->data.data);
            free(bus->channels[i]);
        }
    }
    free(bus->channels);
    free(bus);
}

/* ====== Открытие канала ====== */
/* Если ранее был закрытый слот, он переиспользуется */
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
    channel->closed = 0;

    int index = -1;
    for (int i = 0; i < bus->channel_count; ++i) {
        if (bus->channels[i] == NULL) {
            index = i;
            break;
        }
    }
    if (index == -1) {
        index = bus->channel_count;
        bus->channels = realloc(bus->channels, sizeof(*bus->channels) * (bus->channel_count + 1));
        bus->channel_count++;
    }
    bus->channels[index] = channel;
    return index;
}

/* ====== Закрытие канала ====== */
/* Слот становится свободным (устанавливаем NULL) */
void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
    if (channel < 0 || channel >= bus->channel_count || bus->channels[channel] == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    ch->closed = 1;
    while (!rlist_empty(&ch->send_queue.coros)) {
        struct wakeup_entry *entry = rlist_first_entry(&ch->send_queue.coros,
                                                       struct wakeup_entry, base);
        coro_wakeup(entry->coro);
        rlist_del_entry(entry, base);
    }
    while (!rlist_empty(&ch->recv_queue.coros)) {
        struct wakeup_entry *entry = rlist_first_entry(&ch->recv_queue.coros,
                                                       struct wakeup_entry, base);
        coro_wakeup(entry->coro);
        rlist_del_entry(entry, base);
    }
    free(ch->data.data);
    free(ch);
    bus->channels[channel] = NULL;
}

/* Вспомогательная функция для проверки дескриптора */
static int check_channel(struct coro_bus *bus, int channel, struct coro_bus_channel **pch) {
    if (channel < 0 || channel >= bus->channel_count || bus->channels[channel] == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    *pch = bus->channels[channel];
    if ((*pch)->closed) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    return 0;
}

/* ====== Операции send/recv ====== */
int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
    struct coro_bus_channel *ch;
    if (check_channel(bus, channel, &ch) < 0)
        return -1;

    while (1) {
        if (ch->closed) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        if (ch->data.size < ch->size_limit) {
            data_vector_append(&ch->data, data);
            wakeup_queue_wakeup_all(&ch->recv_queue);
            return 0;
        }
        wakeup_queue_suspend_this(&ch->send_queue);
    }
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
    struct coro_bus_channel *ch;
    if (check_channel(bus, channel, &ch) < 0)
        return -1;

    if (ch->data.size >= ch->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    data_vector_append(&ch->data, data);
    wakeup_queue_wakeup_all(&ch->recv_queue);
    return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    struct coro_bus_channel *ch;
    if (check_channel(bus, channel, &ch) < 0)
        return -1;

    while (1) {
        if (ch->closed) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        if (ch->data.size > 0) {
            *data = data_vector_pop_first(&ch->data);
            wakeup_queue_wakeup_all(&ch->send_queue);
            return 0;
        }
        wakeup_queue_suspend_this(&ch->recv_queue);
    }
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    struct coro_bus_channel *ch;
    if (check_channel(bus, channel, &ch) < 0)
        return -1;

    if (ch->data.size == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    *data = data_vector_pop_first(&ch->data);
    wakeup_queue_wakeup_all(&ch->send_queue);
    return 0;
}

/* ====== Бонус: Broadcast ====== */
#if NEED_BROADCAST
int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
    if (bus->channel_count == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    /* Если хотя бы один открытый канал заполнен, блокируемся */
    while (1) {
        int any_full = 0;
        for (int i = 0; i < bus->channel_count; ++i) {
            struct coro_bus_channel *ch = bus->channels[i];
            if (ch == NULL || ch->closed)
                continue;
            if (ch->data.size >= ch->size_limit) {
                any_full = 1;
                wakeup_queue_suspend_this(&ch->send_queue);
                break;
            }
        }
        if (!any_full)
            break;
    }
    int sent = 0;
    for (int i = 0; i < bus->channel_count; ++i) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch == NULL || ch->closed)
            continue;
        data_vector_append(&ch->data, data);
        wakeup_queue_wakeup_all(&ch->recv_queue);
        sent = 1;
    }
    if (!sent) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
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
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch == NULL || ch->closed)
            continue;
        if (ch->data.size >= ch->size_limit) {
            coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
            return -1;
        }
    }
    int sent = 0;
    for (int i = 0; i < bus->channel_count; ++i) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch == NULL || ch->closed)
            continue;
        data_vector_append(&ch->data, data);
        wakeup_queue_wakeup_all(&ch->recv_queue);
        sent = 1;
    }
    if (!sent) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    return 0;
}
#endif

/* ====== Бонус: Batch-операции ====== */
#if NEED_BATCH
int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
    struct coro_bus_channel *ch;
    if (check_channel(bus, channel, &ch) < 0)
        return -1;
    /* Если канал заполнен, блокируемся до появления хотя бы одного свободного места */
    if (ch->data.size >= ch->size_limit) {
        while (ch->data.size >= ch->size_limit) {
            if (ch->closed) {
                coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
                return -1;
            }
            wakeup_queue_suspend_this(&ch->send_queue);
        }
    }
    size_t available = ch->size_limit - ch->data.size;
    size_t to_send = (available < count) ? available : count;
    data_vector_append_many(&ch->data, data, to_send);
    wakeup_queue_wakeup_all(&ch->recv_queue);
    return to_send;
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
    struct coro_bus_channel *ch;
    if (check_channel(bus, channel, &ch) < 0)
        return -1;
    size_t available = ch->size_limit - ch->data.size;
    if (available == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    size_t to_send = (available < count) ? available : count;
    data_vector_append_many(&ch->data, data, to_send);
    wakeup_queue_wakeup_all(&ch->recv_queue);
    return to_send;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
    struct coro_bus_channel *ch;
    if (check_channel(bus, channel, &ch) < 0)
        return -1;
    while (ch->data.size == 0) {
        if (ch->closed) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        wakeup_queue_suspend_this(&ch->recv_queue);
    }
    size_t to_recv = ch->data.size;
    if (to_recv > capacity)
        to_recv = capacity;
    data_vector_pop_first_many(&ch->data, data, to_recv);
    wakeup_queue_wakeup_all(&ch->send_queue);
    return to_recv;
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
    struct coro_bus_channel *ch;
    if (check_channel(bus, channel, &ch) < 0)
        return -1;
    if (ch->data.size == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    size_t to_recv = ch->data.size;
    if (to_recv > capacity)
        to_recv = capacity;
    data_vector_pop_first_many(&ch->data, data, to_recv);
    wakeup_queue_wakeup_all(&ch->send_queue);
    return to_recv;
}
#endif