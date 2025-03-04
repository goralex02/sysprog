#include "parser.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>

static char **build_argv(const struct command *cmd) {
    char **argv = malloc((cmd->arg_count + 2) * sizeof(char *));
    if (!argv) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    argv[0] = cmd->exe;
    for (uint32_t i = 0; i < cmd->arg_count; ++i)
        argv[i + 1] = cmd->args[i];
    argv[cmd->arg_count + 1] = NULL;
    return argv;
}

// Функция для перенаправления вывода (в дочернем процессе)
static void redirect_output(const struct command_line *line) {
    if (line->out_type == OUTPUT_TYPE_FILE_NEW || line->out_type == OUTPUT_TYPE_FILE_APPEND) {
        int flags = O_WRONLY | O_CREAT | (line->out_type == OUTPUT_TYPE_FILE_NEW ? O_TRUNC : O_APPEND);
        int fd = open(line->out_file, flags, 0644);
        if (fd == -1) {
            perror("open");
            exit(EXIT_FAILURE);
        }
        if (dup2(fd, STDOUT_FILENO) == -1) {
            perror("dup2");
            close(fd);
            exit(EXIT_FAILURE);
        }
        close(fd);
    }
}

// Функция для выполнения конвейера; возвращает exit code последней команды.
static int execute_pipeline(const struct command_line *line) {
    int num_cmds = 0;
    for (const struct expr *cur = line->head; cur; cur = cur->next)
        if (cur->type == EXPR_TYPE_COMMAND)
            num_cmds++;
    if (num_cmds == 0)
        return 0;
    
    int pipes[num_cmds - 1][2];
    for (int i = 0; i < num_cmds - 1; i++) {
        if (pipe(pipes[i]) == -1) {
            perror("pipe");
            exit(EXIT_FAILURE);
        }
    }
    
    pid_t last_pid = -1;
    int cmd_index = 0;
    for (const struct expr *cur = line->head; cur; cur = cur->next) {
        if (cur->type != EXPR_TYPE_COMMAND)
            continue;
        pid_t pid = fork();
        if (pid == -1) {
            perror("fork");
            exit(EXIT_FAILURE);
        }
        if (pid == 0) {
            // Дочерний процесс
            if (cmd_index > 0) {
                if (dup2(pipes[cmd_index - 1][0], STDIN_FILENO) == -1) {
                    perror("dup2");
                    _exit(EXIT_FAILURE);
                }
            }
            if (cmd_index < num_cmds - 1) {
                if (dup2(pipes[cmd_index][1], STDOUT_FILENO) == -1) {
                    perror("dup2");
                    _exit(EXIT_FAILURE);
                }
            } else {
                redirect_output(line);
            }
            for (int i = 0; i < num_cmds - 1; i++) {
                close(pipes[i][0]);
                close(pipes[i][1]);
            }
            const struct command *cmd = &cur->cmd;
            if (strcmp(cmd->exe, "exit") == 0) {
                int exit_code = 0;
                if (cmd->arg_count > 0)
                    exit_code = atoi(cmd->args[0]);
                _exit(exit_code);
            }
            char **args = build_argv(cmd);
            execvp(cmd->exe, args);
            perror("execvp");
            free(args);
            _exit(EXIT_FAILURE);
        } else {
            last_pid = pid;
            // Родитель сразу закрывает дескрипторы предыдущего пайпа
            if (cmd_index > 0) {
                close(pipes[cmd_index - 1][0]);
                close(pipes[cmd_index - 1][1]);
            }
        }
        cmd_index++;
    }
    for (int i = 0; i < num_cmds - 1; i++) {
        close(pipes[i][0]);
        close(pipes[i][1]);
    }
    int last_status = 0;
    // Ожидаем завершения всех дочерних процессов; запоминаем статус последнего
    for (int i = 0; i < num_cmds; i++) {
        int status;
        pid_t pid = wait(&status);
        if (pid == last_pid) {
            last_status = WIFEXITED(status) ? WEXITSTATUS(status) : 1;
        }
    }
    return last_status;
}

// Вспомогательная функция для пропуска цепочки команд до следующего оператора
static const struct expr *skip_chain(const struct expr *e) {
    const struct expr *tmp = e;
    while (tmp && tmp->type == EXPR_TYPE_COMMAND)
        tmp = tmp->next;
    return tmp;
}

/// Функция для выполнения командной строки; возвращает exit code последней команды.
static int execute_command_line(const struct command_line *line) {
    assert(line != NULL);
    if (line->head == NULL)
        return 0;
    
    // Если команда единственная – обрабатываем встроенные команды
    if (line->head->next == NULL && line->head->type == EXPR_TYPE_COMMAND) {
        const struct command *cmd = &line->head->cmd;
        if (strcmp(cmd->exe, "cd") == 0) {
            if (cmd->arg_count < 1) {
                fprintf(stderr, "cd: missing argument\n");
                return 1;
            } else if (chdir(cmd->args[0]) == -1) {
                perror("chdir");
                return 1;
            }
            return 0;
        }
        if (strcmp(cmd->exe, "exit") == 0) {
            int exit_code = 0;
            if (cmd->arg_count > 0)
                exit_code = atoi(cmd->args[0]);
            exit(exit_code);
        }
    }
    
    // Если присутствует пайп, выполняем его и возвращаем код последней команды
    bool has_pipeline = false;
    for (const struct expr *e = line->head; e; e = e->next) {
        if (e->type == EXPR_TYPE_PIPE) {
            has_pipeline = true;
            break;
        }
    }
    if (has_pipeline)
        return execute_pipeline(line);
    
    // Обработка логических операторов (|| и &&) для простых команд
    int last_status = 0;
    const struct expr *e = line->head;
    while (e) {
        if (e->type == EXPR_TYPE_COMMAND) {
            const struct command *cmd = &e->cmd;
            if (strcmp(cmd->exe, "cd") == 0) {
                if (cmd->arg_count < 1) {
                    fprintf(stderr, "cd: missing argument\n");
                    last_status = 1;
                } else if (chdir(cmd->args[0]) == -1) {
                    perror("chdir");
                    last_status = 1;
                } else {
                    last_status = 0;
                }
                e = e->next;
                continue;
            }
            if (strcmp(cmd->exe, "exit") == 0) {
                int exit_code = 0;
                if (cmd->arg_count > 0)
                    exit_code = atoi(cmd->args[0]);
                exit(exit_code);
            }
            pid_t pid = fork();
            if (pid == -1) {
                perror("fork");
                exit(EXIT_FAILURE);
            }
            if (pid == 0) {
                // В дочернем процессе перенаправляем вывод, если требуется
                if (line->out_type == OUTPUT_TYPE_FILE_NEW || line->out_type == OUTPUT_TYPE_FILE_APPEND)
                    redirect_output(line);
                char **args = build_argv(cmd);
                execvp(cmd->exe, args);
                perror("execvp");
                free(args);
                _exit(EXIT_FAILURE);
            } else {
                int status;
                waitpid(pid, &status, 0);
                if (WIFEXITED(status))
                    last_status = WEXITSTATUS(status);
                else
                    last_status = 1;
            }
            e = e->next;
        } else if (e->type == EXPR_TYPE_OR) {
            if (last_status == 0)
                e = skip_chain(e->next);
            else
                e = e->next;
        } else if (e->type == EXPR_TYPE_AND) {
            if (last_status != 0)
                e = skip_chain(e->next);
            else
                e = e->next;
        } else {
            e = e->next;
        }
    }
    return last_status;
}

int main(void) {
    const size_t buf_size = 1024;
    char buf[buf_size];
    int rc;
    struct parser *p = parser_new();
    int last_exit = 0;
    
    while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
        parser_feed(p, buf, rc);
        struct command_line *line = NULL;
        while (true) {
            enum parser_error err = parser_pop_next(p, &line);
            if (err == PARSER_ERR_NONE && line == NULL)
                break;
            if (err != PARSER_ERR_NONE) {
                printf("Error: %d\n", (int)err);
                continue;
            }
            last_exit = execute_command_line(line);
            command_line_delete(line);
        }
    }
    
    parser_delete(p);
    return last_exit;
}