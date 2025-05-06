#include "utils.h"
#include <string.h>

void buffer_init(struct buffer *b) {
    b->data = NULL;
    b->used = 0;
    b->sent = 0;
}

void buffer_free(struct buffer *b) {
    free(b->data);
    b->data = NULL;
    b->used = b->sent = 0;
}

int buffer_append(struct buffer *b, const char *data, size_t len) {
    size_t need = b->used + len;
    char *new_data = realloc(b->data, need);
    if (!new_data) return -1;
    memcpy(new_data + b->used, data, len);
    b->data = new_data;
    b->used = need;
    return 0;
}

void buffer_consume(struct buffer *b, size_t n) {
    if (n >= b->used) {
        b->used = 0;
        b->sent = 0;
        return;
    }
    size_t rem = b->used - n;
    memmove(b->data, b->data + n, rem);
    b->used = rem;
    if (b->sent > n) b->sent -= n;
    else b->sent = 0;
}

void buffer_clear(struct buffer *b) {
    b->used = b->sent = 0;
}

void queue_init(struct msg_queue *q) {
    q->head = q->tail = NULL;
}

void queue_free(struct msg_queue *q) {
    struct msg_node *cur = q->head;
    while (cur) {
        struct msg_node *next = cur->next;
        chat_message_delete(cur->msg);
        free(cur);
        cur = next;
    }
    q->head = q->tail = NULL;
}

int queue_enqueue(struct msg_queue *q, struct chat_message *msg) {
    struct msg_node *node = malloc(sizeof(*node));
    if (!node) return -1;
    node->msg = msg;
    node->next = NULL;
    if (q->tail) q->tail->next = node;
    else q->head = node;
    q->tail = node;
    return 0;
}

struct chat_message *queue_dequeue(struct msg_queue *q) {
    if (!q->head) return NULL;
    struct msg_node *node = q->head;
    struct chat_message *msg = node->msg;
    q->head = node->next;
    if (!q->head) q->tail = NULL;
    free(node);
    return msg;
}

int is_empty_message(const char *msg, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        if (!isspace((unsigned char)msg[i]))
            return 0;
    }
    return 1;
}