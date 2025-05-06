#include "chat_server.h"
#include "utils.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <ctype.h>

// Структура для хранения информации о клиенте
struct chat_peer {
    int socket;
    struct buffer inbuf;
    struct buffer outbuf;
    char *name;  // Имя клиента
};

// Основная структура сервера
struct chat_server {
    int listen_sock;
    struct pollfd *pfds;
    struct chat_peer **peers;
    size_t npfds;
    struct msg_queue queue;
};

// Установка неблокирующего режима для сокета
static void make_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Проверка, является ли сообщение пустым


// Удаление пробельных символов с начала и конца строки
static void trim_whitespace(char *s) {
    if (!s) return;
    
    char* start = s;
    while (*start && isspace((unsigned char)*start)) start++;
    memmove(s, start, strlen(start) + 1);
    
    char* end = s + strlen(s) - 1;
    while (end >= s && isspace((unsigned char)*end)) *end-- = '\0';
}

// Создание нового сервера
struct chat_server* chat_server_new(void) {
    struct chat_server* s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    
    s->listen_sock = -1;
    s->pfds = NULL;
    s->peers = NULL;
    s->npfds = 0;
    queue_init(&s->queue);
    return s;
}

// Освобождение ресурсов сервера
void chat_server_delete(struct chat_server* s) {
    if (!s) return;
    
    if (s->listen_sock >= 0) 
        close(s->listen_sock);
        
    for (size_t i = 1; i < s->npfds; ++i) {
        struct chat_peer* pp = s->peers[i];
        if (pp) {
            close(pp->socket);
            buffer_free(&pp->inbuf);
            buffer_free(&pp->outbuf);
            free(pp->name);
            free(pp);
        }
    }
    
    free(s->peers);
    free(s->pfds);
    queue_free(&s->queue);
    free(s);
}

// Запуск сервера на указанном порту
int chat_server_listen(struct chat_server* s, uint16_t port) {
    if (!s || s->listen_sock >= 0) 
        return CHAT_ERR_ALREADY_STARTED;
        
    int ls = socket(AF_INET, SOCK_STREAM | O_NONBLOCK, 0);
    if (ls < 0) 
        return CHAT_ERR_SYS;
        
    int opt = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in addr = { 
        .sin_family = AF_INET, 
        .sin_addr.s_addr = INADDR_ANY, 
        .sin_port = htons(port) 
    };
    
    if (bind(ls, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(ls); 
        return CHAT_ERR_PORT_BUSY;
    }
    
    if (listen(ls, SOMAXCONN) < 0) {
        close(ls); 
        return CHAT_ERR_SYS;
    }
    
    make_nonblocking(ls);
    
    s->pfds = malloc(sizeof(*s->pfds));
    s->peers = malloc(sizeof(*s->peers));
    
    if (!s->pfds || !s->peers) {
        free(s->pfds);
        free(s->peers);
        close(ls);
        return CHAT_ERR_SYS;
    }
    
    s->pfds[0] = (struct pollfd){ .fd = ls, .events = POLLIN };
    s->peers[0] = NULL;
    s->listen_sock = ls;
    s->npfds = 1;
    
    return 0;
}

// Основной цикл обработки событий сервера
int chat_server_update(struct chat_server* s, double timeout) {
    if (!s || s->listen_sock < 0) 
        return CHAT_ERR_NOT_STARTED;
        
    // Обновление событий
    s->pfds[0].events = POLLIN;
    
    for (size_t i = 1; i < s->npfds; ++i) {
        s->pfds[i].events = POLLIN;
        struct chat_peer* pp = s->peers[i];
        if (pp && pp->outbuf.used > pp->outbuf.sent)
            s->pfds[i].events |= POLLOUT;
    }
    
    // Ожидание событий
    int rc = poll(s->pfds, s->npfds, timeout < 0 ? -1 : (int)(timeout * 1000));
    if (rc == 0) return CHAT_ERR_TIMEOUT;
    if (rc < 0) return CHAT_ERR_SYS;
    
    // Обработка событий
    for (size_t i = 0; i < s->npfds; ++i) {
        struct pollfd* p = &s->pfds[i];
        if (!p->revents) continue;
        
        if (i == 0) {  // Новое подключение
            int client_sock = accept(s->listen_sock, NULL, NULL);
            if (client_sock >= 0) {
                make_nonblocking(client_sock);
                
                struct pollfd* new_pfds = realloc(s->pfds, sizeof(*s->pfds) * (s->npfds + 1));
                struct chat_peer** new_peers = realloc(s->peers, sizeof(*s->peers) * (s->npfds + 1));
                
                if (!new_pfds || !new_peers) {
                    close(client_sock);
                    return CHAT_ERR_SYS;
                }
                
                s->pfds = new_pfds;
                s->peers = new_peers;
                
                struct chat_peer* pp = calloc(1, sizeof(*pp));
                if (!pp) {
                    close(client_sock);
                    return CHAT_ERR_SYS;
                }
                
                pp->socket = client_sock;
                pp->name = NULL;
                buffer_init(&pp->inbuf);
                buffer_init(&pp->outbuf);
                
                s->pfds[s->npfds] = (struct pollfd){ .fd = client_sock, .events = POLLIN };
                s->peers[s->npfds] = pp;
                s->npfds++;
            }
        } 
        else {  // Работа с существующим клиентом
            struct chat_peer* pp = s->peers[i];
            if (!pp) continue;
            
            if (p->revents & POLLIN) {  // Получение данных
                char buf[4096];
                ssize_t n = read(pp->socket, buf, sizeof(buf));
                
                if (n <= 0) {  // Отключение клиента
                    close(pp->socket);
                    buffer_free(&pp->inbuf);
                    buffer_free(&pp->outbuf);
                    free(pp->name);
                    free(pp);
                    s->peers[i] = NULL;
                    
                    // Перемещение последнего элемента на место удаленного
                    if (i < s->npfds - 1) {
                        s->pfds[i] = s->pfds[s->npfds - 1];
                        s->peers[i] = s->peers[s->npfds - 1];
                    }
                    
                    s->npfds--;
                    i--;  // Повторная проверка текущей позиции
                    continue;
                }
                
                // Обработка полученных данных
                if (buffer_append(&pp->inbuf, buf, n) < 0) {
                    return CHAT_ERR_SYS;
                }
                
                char* data = pp->inbuf.data;
                size_t used = pp->inbuf.used;
                size_t start = 0;
                
                for (size_t j = 0; j < used; ++j) {
                    if (data[j] == '\n') {
                        char *line = strndup(data + start, j - start);
                        if (!line) continue;
                        
                        trim_whitespace(line);
                        
                        // Обработка AUTH-сообщения
                        if (!pp->name && strncmp(line, "AUTH ", 5) == 0) {
							pp->name = strdup(line + 5);
						} else if (*line) {
							// Проверка на пустое сообщение
							if (is_empty_message(line, strlen(line))) {
								free(line);
								continue;
							}
						
							struct chat_message* msg = malloc(sizeof(*msg));
							if (!msg) {
								free(line);
								continue;
							}
						
							msg->data = line;
						#if NEED_AUTHOR
							msg->author = pp->name ? strdup(pp->name) : NULL;
						#endif
							queue_enqueue(&s->queue, msg);
						
							// Рассылка всем клиентам
							for (size_t k = 1; k < s->npfds; ++k) {
								if (k == i) continue;
								struct chat_peer* dest = s->peers[k];
								if (dest) {
									buffer_append(&dest->outbuf, line, strlen(line));
									buffer_append(&dest->outbuf, "\n", 1);
								}
							}
						} else {
							free(line);
						}
                        
                        start = j + 1;
                    }
                }
                
                if (start > 0)
                    buffer_consume(&pp->inbuf, start);
            }
            
            // Отправка данных
            if (p->revents & POLLOUT) {
                size_t to_send = pp->outbuf.used - pp->outbuf.sent;
                ssize_t n = write(pp->socket, pp->outbuf.data + pp->outbuf.sent, to_send);
                
                if (n > 0) {
                    pp->outbuf.sent += n;
                    if (pp->outbuf.sent == pp->outbuf.used)
                        buffer_clear(&pp->outbuf);
                } else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    return CHAT_ERR_SYS;
                }
            }
        }
    }
    
    return 0;
}

// Получение следующего сообщения из очереди
struct chat_message* chat_server_pop_next(struct chat_server* s) {
    return s ? queue_dequeue(&s->queue) : NULL;
}

// Получение дескриптора сервера
int chat_server_get_descriptor(const struct chat_server* s) {
    return s ? s->listen_sock : -1;
}

// Получение сокета сервера
int chat_server_get_socket(const struct chat_server* s) {
    return s ? s->listen_sock : -1;
}

// Получение событий сервера
int chat_server_get_events(const struct chat_server* s) {
    if (!s) return 0;
    int ev = 0;
    
    // Добавляем INPUT, если сервер запущен
    if (s->listen_sock >= 0)
        ev |= CHAT_EVENT_INPUT;
        
    // Добавляем OUTPUT, если есть клиенты с данными
    for (size_t i = 1; i < s->npfds; ++i) {
        struct chat_peer* pp = s->peers[i];
        if (pp && pp->outbuf.used > pp->outbuf.sent)
            ev |= CHAT_EVENT_OUTPUT;
    }
    
    return ev;
}

// Отправка сообщения от сервера
int chat_server_feed(struct chat_server *s, const char *msg, uint32_t msg_size) {
#if NEED_SERVER_FEED
    if (!s || !msg || msg_size == 0)
        return CHAT_ERR_INVALID_ARGUMENT;
    
    // Проверка на пустое сообщение
    char *temp = strndup(msg, msg_size);
    if (!temp) return CHAT_ERR_SYS;
    
    trim_whitespace(temp);
    int is_empty = (*temp == '\0');
    free(temp);
    
    if (is_empty)
        return 0;
    
    // Добавление сообщения в очередь сервера
    struct chat_message *server_msg = malloc(sizeof(*server_msg));
    if (!server_msg) return CHAT_ERR_SYS;
    
    server_msg->data = strndup(msg, msg_size);
#if NEED_AUTHOR
    server_msg->author = strdup("server");
#endif
    queue_enqueue(&s->queue, server_msg);
    
    // Рассылка сообщения всем клиентам
    for (size_t i = 1; i < s->npfds; ++i) {
        struct chat_peer* pp = s->peers[i];
        if (pp) {
            buffer_append(&pp->outbuf, msg, msg_size);
            buffer_append(&pp->outbuf, "\n", 1);
        }
    }
    
    return 0;
#else
    return CHAT_ERR_NOT_IMPLEMENTED;
#endif
}