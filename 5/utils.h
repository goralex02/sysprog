#pragma once
#include <stdlib.h>
#include <stdint.h>
#include <ctype.h>

// Dynamic byte buffer for I/O
// utils.h

// Dynamic byte buffer for I/O
struct buffer {
    char *data;
    size_t used;
    size_t sent;
};

void buffer_init(struct buffer *b);
void buffer_free(struct buffer *b);
int  buffer_append(struct buffer *b, const char *data, size_t len);
void buffer_consume(struct buffer *b, size_t n);
void buffer_clear(struct buffer *b);

// Simple FIFO queue for chat_message*
#include "chat.h"

struct msg_node {
    struct chat_message *msg;
    struct msg_node *next;
};

struct msg_queue {
    struct msg_node *head;
    struct msg_node *tail;
};

void queue_init(struct msg_queue *q);
void queue_free(struct msg_queue *q);
int  queue_enqueue(struct msg_queue *q, struct chat_message *msg);
struct chat_message *queue_dequeue(struct msg_queue *q);

// Функция проверки пустоты сообщения
int is_empty_message(const char *msg, size_t len);  // <-- Добавьте это