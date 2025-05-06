#define _POSIX_C_SOURCE 200809L
#include "chat_client.h"
#include "utils.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <poll.h>
#include <stdio.h>
#include <ctype.h>

struct chat_client {
    int socket;
    struct buffer inbuf;
    struct buffer outbuf;
    struct msg_queue queue;
#if NEED_AUTHOR
    char *name; // Поле для хранения имени клиента
#endif
};

static void trim_whitespace(char *s) {
    if (!s) return;
    
    char *start = s;
    while (*start && isspace((unsigned char)*start)) start++;
    memmove(s, start, strlen(start) + 1);
    
    char *end = s + strlen(s) - 1;
    while (end >= s && isspace((unsigned char)*end)) *end-- = '\0';
}

struct chat_client *chat_client_new(const char *name) {
    struct chat_client *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    
    c->socket = -1;
    buffer_init(&c->inbuf);
    buffer_init(&c->outbuf);
    queue_init(&c->queue);
    
#if NEED_AUTHOR
    c->name = name ? strdup(name) : NULL;
    if (name && !c->name) {
        free(c);
        return NULL;
    }
#endif
    
    return c;
}

void chat_client_delete(struct chat_client *c) {
    if (!c) return;
    
    if (c->socket >= 0) close(c->socket);
    
    buffer_free(&c->inbuf);
    buffer_free(&c->outbuf);
    queue_free(&c->queue);
    
#if NEED_AUTHOR
    free(c->name);
#endif
    
    free(c);
}

int chat_client_connect(struct chat_client *c, const char *addr) {
    if (!c || c->socket >= 0) 
        return CHAT_ERR_ALREADY_STARTED;
    
    char *hostport = strdup(addr);
    if (!hostport) return CHAT_ERR_SYS;
    
    char *colon = strrchr(hostport, ':');
    if (!colon) { 
        free(hostport); 
        return CHAT_ERR_INVALID_ARGUMENT; 
    }
    
    *colon = '\0'; 
    char *host = hostport;
    char *portstr = colon + 1;
    
    struct addrinfo hints = { 
        .ai_family = AF_INET, 
        .ai_socktype = SOCK_STREAM 
    };
    struct addrinfo *res;
    
    int rc = getaddrinfo(host, portstr, &hints, &res);
    free(hostport);
    
    if (rc != 0) return CHAT_ERR_NO_ADDR;
    
    int sock = socket(res->ai_family, SOCK_STREAM | O_NONBLOCK, 0);
    if (sock < 0) { 
        freeaddrinfo(res);
        return CHAT_ERR_SYS; 
    }
    
    int connect_result = connect(sock, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    
    if (connect_result < 0 && errno != EINPROGRESS) {
        close(sock);
        return CHAT_ERR_SYS;
    }
    
    c->socket = sock;
    
    // Отправка имени клиента на сервер после подключения
#if NEED_AUTHOR
    if (c->name) {
        char auth_msg[1024];
        int len = snprintf(auth_msg, sizeof(auth_msg), "AUTH %s\n", c->name);
        if (buffer_append(&c->outbuf, auth_msg, len) < 0) {
            close(sock);
            return CHAT_ERR_SYS;
        }
    }
#endif
    
    return 0;
}

int chat_client_get_events(const struct chat_client *c) {
    if (!c || c->socket < 0)
        return 0;
        
    int ev = CHAT_EVENT_INPUT;
    if (c->outbuf.used > c->outbuf.sent) 
        ev |= CHAT_EVENT_OUTPUT;
    return ev;
}

int chat_client_update(struct chat_client *c, double timeout) {
    if (!c || c->socket < 0) 
        return CHAT_ERR_NOT_STARTED;
    
    struct pollfd pfd = {
        .fd = c->socket,
        .events = chat_events_to_poll_events(chat_client_get_events(c))
    };
    
    int timeout_ms = timeout < 0 ? -1 : (int)(timeout * 1000);
    int rc = poll(&pfd, 1, timeout_ms);
    
    if (rc == 0) return CHAT_ERR_TIMEOUT;
    if (rc < 0) return CHAT_ERR_SYS;

    // Обработка входящих данных
    if (pfd.revents & POLLIN) {
        char buf[4096];
        ssize_t n = read(c->socket, buf, sizeof(buf));
        
        if (n <= 0) {
            if (n == 0) {
                // Соединение закрыто
                close(c->socket);
                c->socket = -1;
            }
            return CHAT_ERR_SYS;
        }
        
        if (buffer_append(&c->inbuf, buf, n) < 0) {
            return CHAT_ERR_SYS;
        }
        
        char *data = c->inbuf.data;
        size_t used = c->inbuf.used;
        size_t start = 0;
        
        for (size_t i = 0; i < used; ++i) {
            if (data[i] == '\n') {
                char *line = strndup(data + start, i - start);
                if (!line) continue;
                
                trim_whitespace(line);
                
                if (*line) {
                    struct chat_message *msg = malloc(sizeof(*msg));
                    if (!msg) {
                        free(line);
                        continue;
                    }
                    
                    msg->data = line;
#if NEED_AUTHOR
                    msg->author = c->name ? strdup(c->name) : NULL;
#endif
                    queue_enqueue(&c->queue, msg);
                } else {
                    free(line);
                }
                
                start = i + 1;
            }
        }
        
        if (start > 0)
            buffer_consume(&c->inbuf, start);
    }

    // Обработка исходящих данных
    if (pfd.revents & POLLOUT) {
        size_t to_send = c->outbuf.used - c->outbuf.sent;
        ssize_t n = write(c->socket, c->outbuf.data + c->outbuf.sent, to_send);
        
        if (n > 0) {
            c->outbuf.sent += n;
            if (c->outbuf.sent == c->outbuf.used)
                buffer_clear(&c->outbuf);
        } else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            return CHAT_ERR_SYS;
        }
    }
    
    return 0;
}

int chat_client_feed(struct chat_client *c, const char *msg, uint32_t msg_size) {
    if (!c || c->socket < 0)
        return CHAT_ERR_NOT_STARTED;

    if (!msg || msg_size == 0)
        return CHAT_ERR_INVALID_ARGUMENT;

    // Проверка на пустое сообщение
    char *temp = strndup(msg, msg_size);
    if (!temp) return CHAT_ERR_SYS;

    trim_whitespace(temp);
    int is_empty = is_empty_message(temp, strlen(temp));
    free(temp);

    if (is_empty)
        return 0;

    return buffer_append(&c->outbuf, msg, msg_size) < 0 ? CHAT_ERR_SYS : 0;
}

int chat_client_get_descriptor(const struct chat_client *c) {
    return c ? c->socket : -1;
}

struct chat_message *chat_client_pop_next(struct chat_client *c) {
    return c ? queue_dequeue(&c->queue) : NULL;
}