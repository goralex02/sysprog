#include "userfs.h"
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

enum {
    BLOCK_SIZE = 512,
    MAX_FILE_SIZE = 1024 * 1024 * 100,
};

static enum ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
    char *memory;
    size_t occupied;
    struct block *next;
    struct block *prev;
};

struct file {
    struct block *block_list;
    struct block *last_block;
    int refs;
    char *name;
    struct file *next;
    struct file *prev;
    size_t size;
    bool deleted;
    int open_flags;
};

static struct file *file_list = NULL;

struct filedesc {
    struct file *file;
    size_t pos;
    int flags;
};

static struct filedesc **file_descriptors = NULL;
static int file_descriptor_count = 0;
static int file_descriptor_capacity = 0;

static void set_error(enum ufs_error_code error) {
    ufs_error_code = error;
}

static struct file *find_file(const char *filename) {
    for (struct file *f = file_list; f != NULL; f = f->next) {
        if (!f->deleted && strcmp(f->name, filename) == 0) {
            return f;
        }
    }
    return NULL;
}

static struct block *create_block() {
    struct block *blk = malloc(sizeof(struct block));
    if (!blk) {
        set_error(UFS_ERR_NO_MEM);
        return NULL;
    }
    
    blk->memory = malloc(BLOCK_SIZE);
    if (!blk->memory) {
        free(blk);
        set_error(UFS_ERR_NO_MEM);
        return NULL;
    }
    
    blk->occupied = 0;
    blk->next = blk->prev = NULL;
    return blk;
}

static void free_block(struct block *blk) {
    if (!blk) return;
    free(blk->memory);
    free(blk);
}

static struct file *create_file(const char *filename) {
    struct file *new_file = malloc(sizeof(struct file));
    if (!new_file) {
        set_error(UFS_ERR_NO_MEM);
        return NULL;
    }

    new_file->name = strdup(filename);
    if (!new_file->name) {
        free(new_file);
        set_error(UFS_ERR_NO_MEM);
        return NULL;
    }

    new_file->block_list = NULL;
    new_file->last_block = NULL;
    new_file->refs = 0;
    new_file->size = 0;
    new_file->deleted = false;
    new_file->open_flags = 0;
    new_file->next = file_list;
    new_file->prev = NULL;
    
    if (file_list) {
        file_list->prev = new_file;
    }
    file_list = new_file;

    return new_file;
}

static void delete_file(struct file *file) {
    if (!file)
        return;

    // Освобождаем список блоков
    struct block *blk = file->block_list;
    while (blk) {
        struct block *next = blk->next;
        free_block(blk);
        blk = next;
    }

    // Если файл всё ещё присутствует в глобальном списке, отцепляем его
    if (file->prev) {
        file->prev->next = file->next;
    } else if (file_list == file) {
        file_list = file->next;
    }
    if (file->next) {
        file->next->prev = file->prev;
    }

    // Освобождаем имя файла и саму структуру
    free(file->name);
    free(file);
}

static struct block *get_block_at_position(struct file *file, size_t pos, size_t *offset) {
    size_t blk_idx = pos / BLOCK_SIZE;
    struct block *blk = file->block_list;
    
    for (size_t i = 0; i < blk_idx && blk; i++) {
        blk = blk->next;
    }
    
    if (offset) *offset = pos % BLOCK_SIZE;
    return blk;
}

enum ufs_error_code ufs_errno() {
    return ufs_error_code;
}

int ufs_open(const char *filename, int flags) {
    struct file *file = find_file(filename);
    //bool created = false;

    if (!file && (flags & UFS_CREATE)) {
        file = create_file(filename);
        if (!file) return -1;
        //created = true;
    } else if (!file) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    int fd = -1;
    for (int i = 0; i < file_descriptor_capacity; ++i) {
        if (file_descriptors[i] == NULL) {
            fd = i;
            break;
        }
    }

    if (fd == -1) {
        int new_cap = file_descriptor_capacity == 0 ? 16 : file_descriptor_capacity * 2;
        struct filedesc **new_arr = realloc(file_descriptors, new_cap * sizeof(*file_descriptors));
        if (!new_arr) {
            set_error(UFS_ERR_NO_MEM);
            return -1;
        }
        for (int i = file_descriptor_capacity; i < new_cap; ++i) {
            new_arr[i] = NULL;
        }
        file_descriptors = new_arr;
        fd = file_descriptor_capacity;
        file_descriptor_capacity = new_cap;
    }

    int access_flags = flags & (UFS_READ_ONLY | UFS_WRITE_ONLY | UFS_READ_WRITE);
    if (access_flags == 0) {
        access_flags = UFS_READ_WRITE;
    }

    struct filedesc *desc = malloc(sizeof(struct filedesc));
    if (!desc) {
        set_error(UFS_ERR_NO_MEM);
        return -1;
    }

    desc->file = file;
    desc->pos = 0;
    desc->flags = access_flags;

    file_descriptors[fd] = desc;
    file->refs++;
    file_descriptor_count++;

    return fd;
}

ssize_t ufs_write(int fd, const char *buf, size_t size) {
    if (fd < 0 || fd >= file_descriptor_capacity || !file_descriptors[fd]) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    struct filedesc *desc = file_descriptors[fd];
    struct file *file = desc->file;

    // Удалённая проверка: если файл удалён, запись всё равно разрешается,
    // так как открытые дескрипторы должны работать независимо от удаления.
    
    if ((desc->flags & UFS_WRITE_ONLY) == 0 && (desc->flags & UFS_READ_WRITE) == 0) {
        set_error(UFS_ERR_NO_PERMISSION);
        return -1;
    }

    if (file->size + size > MAX_FILE_SIZE) {
        set_error(UFS_ERR_NO_MEM);
        return -1;
    }

    size_t total_written = 0;
    size_t current_pos = desc->pos;

    while (total_written < size) {
        size_t offset_in_blk;
        struct block *blk = get_block_at_position(file, current_pos, &offset_in_blk);
        
        if (!blk) {
            blk = create_block();
            if (!blk) {
                return -1;
            }
            
            if (file->last_block) {
                file->last_block->next = blk;
                blk->prev = file->last_block;
            } else {
                file->block_list = blk;
            }
            file->last_block = blk;
        }

        size_t space_avail = BLOCK_SIZE - offset_in_blk;
        size_t to_write = (size - total_written < space_avail) ? size - total_written : space_avail;

        memcpy(blk->memory + offset_in_blk, buf + total_written, to_write);
        
        if (offset_in_blk + to_write > blk->occupied) {
            blk->occupied = offset_in_blk + to_write;
        }
        
        total_written += to_write;
        current_pos += to_write;
    }

    if (current_pos > file->size) {
        file->size = current_pos;
    }
    desc->pos = current_pos;

    return total_written;
}

ssize_t ufs_read(int fd, char *buf, size_t size) {
    if (fd < 0 || fd >= file_descriptor_capacity || !file_descriptors[fd]) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    struct filedesc *desc = file_descriptors[fd];
    struct file *file = desc->file;

    if ((desc->flags & UFS_READ_ONLY) == 0 && (desc->flags & UFS_READ_WRITE) == 0) {
        set_error(UFS_ERR_NO_PERMISSION);
        return -1;
    }

    if (desc->pos >= file->size) {
        return 0;
    }

    size_t total_read = 0;
    size_t current_pos = desc->pos;
    size_t remaining = file->size - current_pos;
    if (size > remaining) {
        size = remaining;
    }

    while (total_read < size) {
        size_t offset_in_blk;
        struct block *blk = get_block_at_position(file, current_pos, &offset_in_blk);
        if (!blk) break;

        size_t data_avail = blk->occupied - offset_in_blk;
        if (data_avail == 0) break;

        size_t to_read = (size - total_read < data_avail) ? 
                         size - total_read : data_avail;

        memcpy(buf + total_read, blk->memory + offset_in_blk, to_read);
        total_read += to_read;
        current_pos += to_read;
    }

    desc->pos = current_pos;
    return total_read;
}

int ufs_close(int fd) {
    if (fd < 0 || fd >= file_descriptor_capacity || !file_descriptors[fd]) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    struct filedesc *desc = file_descriptors[fd];
    struct file *file = desc->file;

    free(desc);
    file_descriptors[fd] = NULL;
    file_descriptor_count--;

    file->refs--;
    if (file->refs == 0 && file->deleted) {
        delete_file(file);
    }

    return 0;
}

int ufs_delete(const char *filename) {
    struct file *file = find_file(filename);
    if (!file) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }
    
    // Помечаем файл как удалённый.
    file->deleted = true;
    
    // Если файл не имеет открытых дескрипторов, можно удалить его сразу.
    if (file->refs == 0) {
        // Удаляем его из глобального списка:
        if (file->prev)
            file->prev->next = file->next;
        else if (file_list == file)
            file_list = file->next;
        if (file->next)
            file->next->prev = file->prev;
        // Обнуляем ссылки, чтобы не было циклических связей.
        file->next = file->prev = NULL;
        delete_file(file);
    }
    // Если файл всё ещё открыт, оставляем его в списке,
    // чтобы ufs_destroy мог корректно пройти по нему.
    return 0;
}

#if NEED_RESIZE
int ufs_resize(int fd, size_t new_size) {
    if (fd < 0 || fd >= file_descriptor_capacity || !file_descriptors[fd]) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    struct filedesc *desc = file_descriptors[fd];
    struct file *file = desc->file;

    if (new_size > MAX_FILE_SIZE) {
        set_error(UFS_ERR_NO_MEM);
        return -1;
    }

    if (new_size == file->size) {
        return 0;
    }

    if (new_size < file->size) {
        // Вычисляем, сколько блоков потребуется для нового размера
        size_t blocks_needed = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        struct block *blk = file->block_list;
        struct block *last_valid = NULL;

        // Проходим по блокам, оставляем нужное количество
        for (size_t i = 0; i < blocks_needed; ++i) {
            if (!blk)
                break;
            last_valid = blk;
            if (i == blocks_needed - 1) {
                size_t new_occupied = new_size % BLOCK_SIZE;
                if (new_occupied == 0 && new_size != 0)
                    new_occupied = BLOCK_SIZE;
                blk->occupied = new_occupied;
            }
            blk = blk->next;
        }

        // Освобождаем оставшиеся блоки за пределами нового размера
        while (blk) {
            struct block *next = blk->next;
            free_block(blk);
            blk = next;
        }

        // Обновляем указатель на последний блок
        if (last_valid) {
            last_valid->next = NULL;
            file->last_block = last_valid;
        } else {
            file->block_list = NULL;
            file->last_block = NULL;
        }
    } else {
        // При увеличении размера файла добавляем новые блоки
        size_t additional = new_size - file->size;
        while (additional > 0) {
            struct block *blk = create_block();
            if (!blk)
                return -1;
            size_t to_add = (additional > BLOCK_SIZE) ? BLOCK_SIZE : additional;
            blk->occupied = to_add;
            additional -= to_add;

            if (file->last_block) {
                file->last_block->next = blk;
                blk->prev = file->last_block;
            } else {
                file->block_list = blk;
            }
            file->last_block = blk;
        }
    }

    file->size = new_size;
    if (desc->pos > new_size)
        desc->pos = new_size;

    return 0;
}
#endif

void ufs_destroy(void) {
    /*
     * Пройдемся по глобальному списку файлов и освободим все файлы.
     * Теперь все файлы должны находиться в file_list, потому что мы
     * больше не удаляем файлы с открытыми дескрипторами из списка.
     */
    struct file *f = file_list;
    while (f) {
        struct file *next = f->next;
        delete_file(f);
        f = next;
    }
    
    // Освобождаем массив дескрипторов.
    for (int i = 0; i < file_descriptor_capacity; ++i) {
        if (file_descriptors[i])
            free(file_descriptors[i]);
    }
    free(file_descriptors);
    
    file_list = NULL;
    file_descriptors = NULL;
    file_descriptor_count = 0;
    file_descriptor_capacity = 0;
    set_error(UFS_ERR_NO_ERR);
}