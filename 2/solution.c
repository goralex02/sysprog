#include "parser.h"
#include <sys/wait.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>

struct bgproc_list {
    pid_t *jobs;
    int count;
    int capacity;
};

static void bgproc_init(struct bgproc_list *bpl) {
    bpl->jobs = NULL;
    bpl->count = 0;
    bpl->capacity = 0;
}

static void bgproc_add(struct bgproc_list *bpl, pid_t pid) {
    if (bpl->jobs == NULL) {
        bpl->capacity = 16;
        bpl->jobs = malloc(bpl->capacity * sizeof(pid_t));
    } else if (bpl->count == bpl->capacity) {
        bpl->capacity *= 2;
        bpl->jobs = realloc(bpl->jobs, bpl->capacity * sizeof(pid_t));
    }
    bpl->jobs[bpl->count++] = pid;
}

static void bgproc_reap(struct bgproc_list *bpl) {
    int i = 0;
    while (i < bpl->count) {
        int status;
        pid_t w = waitpid(bpl->jobs[i], &status, WNOHANG);
        if (w <= 0) {
            if (w == -1 || (w == bpl->jobs[i] && (WIFEXITED(status) || WIFSIGNALED(status)))) {
                memmove(&bpl->jobs[i], &bpl->jobs[i+1], (bpl->count-i-1)*sizeof(pid_t));
                bpl->count--;
            } else {
                i++;
            }
        } else {
            i++;
        }
    }
}

static void bgproc_free(struct bgproc_list *bpl) {
    free(bpl->jobs);
}

struct pipeline_pids {
    pid_t *pids;
    int size;
    int capacity;
};

static void pipeline_init(struct pipeline_pids *pp, int cap) {
    pp->pids = calloc(cap, sizeof(pid_t));
    pp->capacity = cap;
    pp->size = 0;
}
static void pipeline_add(struct pipeline_pids *pp, pid_t pid) {
    if (pp->size == pp->capacity) {
        pp->capacity *= 2;
        pp->pids = realloc(pp->pids, pp->capacity * sizeof(pid_t));
    }
    pp->pids[pp->size++] = pid;
}
static void pipeline_free(struct pipeline_pids *pp) {
    free(pp->pids);
}

static bool handle_control(struct expr **cur, bool *skip, int *last) {
    struct expr *e = *cur;
    if (e->type == EXPR_TYPE_AND) {
        *skip = (*last != 0);
        *cur = e->next;
        return true;
    } else if (e->type == EXPR_TYPE_OR) {
        *skip = (*last == 0);
        *cur = e->next;
        return true;
    }
    return false;
}

static bool handle_cd(struct expr **cur) {
    struct expr *e = *cur;
    if (e->type == EXPR_TYPE_COMMAND) {
        struct command *cmd = &e->cmd;
        if (strcmp(cmd->exe, "cd") == 0) {
            if (cmd->arg_count) chdir(cmd->args[0]);
            *cur = e->next;
            return true;
        }
    }
    return false;
}

static bool handle_exit_cmd(struct expr **cur, int *exit_code) {
    struct expr *e = *cur;
    if (e->type == EXPR_TYPE_COMMAND) {
        struct command *cmd = &e->cmd;
        if (strcmp(cmd->exe, "exit") == 0 && !e->next) {
            *exit_code = cmd->arg_count ? atoi(cmd->args[0]) : 0;
            *cur = e->next;
            return true;
        }
    }
    return false;
}

static void handle_parent(pid_t pid,
                          bool is_background,
                          bool do_pipe,
                          struct pipeline_pids *pp,
                          struct bgproc_list *bpl,
                          int *last,
                          int *exit_code) {
    if (is_background) {
        bgproc_add(bpl, pid);
    } else if (!do_pipe) {
        int st;
        waitpid(pid, &st, 0);
        if (WIFEXITED(st)) {
            *last = WEXITSTATUS(st);
            *exit_code = *last;
        }
    } else {
        pipeline_add(pp, pid);
    }
}

static void execute_command_line(const struct command_line *line, int *exit_code, struct bgproc_list *bpl) {
    struct expr *cur = line->head;
    enum output_type mode = line->out_type;
    char *outfile = line->out_file;
    if (!cur) return;

    struct pipeline_pids pp;
    pipeline_init(&pp, 16);

    int in_fd = STDIN_FILENO;
    bool skip = false;
    int last = *exit_code;

    while (cur) {
        if (handle_control(&cur, &skip, &last)) continue;
        if (skip) {
            while (cur && cur->type != EXPR_TYPE_AND && cur->type != EXPR_TYPE_OR)
                cur = cur->next;
            skip = false;
            continue;
        }
        if (handle_cd(&cur)) continue;
        if (handle_exit_cmd(&cur, exit_code)) break;

        if (cur->type == EXPR_TYPE_COMMAND) {
            struct command *cmd = &cur->cmd;
            bool do_pipe = (cur->next && cur->next->type == EXPR_TYPE_PIPE);
            bool do_file = (!do_pipe && (mode == OUTPUT_TYPE_FILE_NEW || mode == OUTPUT_TYPE_FILE_APPEND) && outfile);
            int fds[2];
            if (do_pipe) pipe(fds);

            pid_t pid = fork();
            if (pid == 0) {
                if (in_fd != STDIN_FILENO) dup2(in_fd, STDIN_FILENO), close(in_fd);
                if (do_pipe) close(fds[0]), dup2(fds[1], STDOUT_FILENO), close(fds[1]);
                if (do_file) {
                    int flags = O_WRONLY | O_CREAT | (mode == OUTPUT_TYPE_FILE_NEW ? O_TRUNC : O_APPEND);
                    int fd = open(outfile, flags, 0644);
                    if (fd >= 0) dup2(fd, STDOUT_FILENO), close(fd);
                }
                int argc = cmd->arg_count + 2;
                char *argv[argc];
                argv[0] = cmd->exe;
                for (uint32_t i = 0; i < cmd->arg_count; ++i) argv[i+1] = cmd->args[i];
                argv[argc-1] = NULL;
                execvp(cmd->exe, argv);
                _exit(EXIT_FAILURE);
            } else if (pid > 0) {
                handle_parent(pid, line->is_background, do_pipe, &pp, bpl, &last, exit_code);
            }

            if (in_fd != STDIN_FILENO) close(in_fd);
            if (do_pipe) close(fds[1]);
            in_fd = do_pipe ? fds[0] : STDIN_FILENO;
        }
        cur = cur->next;
    }

    for (int i = 0; i < pp.size; ++i) waitpid(pp.pids[i], NULL, 0);
    pipeline_free(&pp);
}

int main(void) {
    struct parser *prs = parser_new();
    struct bgproc_list bpl;
    bgproc_init(&bpl);
    char buf[1024];
    int nread, exit_code = 0;
    
    while ((nread = read(STDIN_FILENO, buf, sizeof(buf))) > 0) {
        parser_feed(prs, buf, nread);
        struct command_line *ln;
        while (parser_pop_next(prs, &ln) == PARSER_ERR_NONE && ln) {
            if (ln->head && ln->head->type == EXPR_TYPE_COMMAND) {
                struct expr *nx = ln->head->next;
                struct command *cmd = &ln->head->cmd;
                if (strcmp(cmd->exe, "exit") == 0 && (!nx || nx->type != EXPR_TYPE_PIPE)) {
                    exit_code = cmd->arg_count ? atoi(cmd->args[0]) : 0;
                    command_line_delete(ln);
                    goto cleanup;
                }
            }
            execute_command_line(ln, &exit_code, &bpl);
            command_line_delete(ln);
            bgproc_reap(&bpl);
        }
    }

cleanup:
    bgproc_free(&bpl);
    parser_delete(prs);
    return exit_code;
}

