#include "userfs.h"
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define BLOCK_SIZE (64 * 1024)
#define MAX_FILE_SIZE (100 * 1024 * 1024)

static enum ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
    char* memory;
    struct block* next;
    struct block* prev;
};

struct file {
    struct block* block_list;
    struct block* last_block;
    int refs;
    char* name;
    struct file* next;
    struct file* prev;
    size_t size;
    bool deleted;
};

static struct file* file_list = NULL;

struct filedesc {
    struct file* file;
    size_t pos;
    int flags;
};

static struct filedesc** file_descriptors = NULL;
static int file_descriptor_count = 0;
static int file_descriptor_capacity = 0;

static void set_error(enum ufs_error_code error) {
    ufs_error_code = error;
}

static struct file* find_file(const char* filename) {
    for (struct file* f = file_list; f != NULL; f = f->next) {
        if (!f->deleted && strcmp(f->name, filename) == 0) {
            return f;
        }
    }
    return NULL;
}

static struct block* create_block() {
    struct block* blk = malloc(sizeof(struct block));
    if (!blk) {
        return NULL;
    }
    
    blk->memory = malloc(BLOCK_SIZE);
    if (!blk->memory) {
        free(blk);
        return NULL;
    }
    memset(blk->memory, 0, BLOCK_SIZE);
    blk->next = NULL;
    blk->prev = NULL;
    return blk;
}

static int extend_file(struct file* file, size_t new_size) {
    if (new_size <= file->size) {
        return 0;
    }
    
    size_t current_blocks = (file->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    size_t needed_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    size_t blocks_to_add = needed_blocks - current_blocks;
    
    for (size_t i = 0; i < blocks_to_add; i++) {
        struct block* blk = create_block();
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
    
    file->size = new_size;
    return 0;
}

static struct block* get_block(struct file* file, size_t block_index) {
    struct block* blk = file->block_list;
    for (size_t i = 0; i < block_index && blk != NULL; i++) {
        blk = blk->next;
    }
    return blk;
}

enum ufs_error_code ufs_errno() {
    return ufs_error_code;
}

int ufs_open(const char* filename, int flags) {
    struct file* file = find_file(filename);
    bool created = false;

    if (!file && (flags & UFS_CREATE)) {
        file = malloc(sizeof(struct file));
        if (!file) {
            set_error(UFS_ERR_NO_MEM);
            return -1;
        }
        
        file->name = strdup(filename);
        if (!file->name) {
            free(file);
            set_error(UFS_ERR_NO_MEM);
            return -1;
        }
        
        file->block_list = NULL;
        file->last_block = NULL;
        file->refs = 0;
        file->size = 0;
        file->deleted = false;
        file->next = file_list;
        file->prev = NULL;
        
        if (file_list != NULL) {
            file_list->prev = file;
        }
        file_list = file;
        created = true;
    }
    
    if (!file) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    int fd = -1;
    for (int i = 0; i < file_descriptor_capacity; i++) {
        if (file_descriptors[i] == NULL) {
            fd = i;
            break;
        }
    }
    
    if (fd == -1) {
        int new_capacity = file_descriptor_capacity == 0 ? 16 : file_descriptor_capacity * 2;
        struct filedesc** new_arr = realloc(file_descriptors, new_capacity * sizeof(*file_descriptors));
        if (!new_arr) {
            set_error(UFS_ERR_NO_MEM);
            if (created) {
                if (file->refs == 0) {
                    if (file->prev) file->prev->next = file->next;
                    if (file->next) file->next->prev = file->prev;
                    if (file_list == file) file_list = file->next;
                    free(file->name);
                    free(file);
                }
            }
            return -1;
        }
        
        for (int i = file_descriptor_capacity; i < new_capacity; i++) {
            new_arr[i] = NULL;
        }
        file_descriptors = new_arr;
        fd = file_descriptor_capacity;
        file_descriptor_capacity = new_capacity;
    }

    struct filedesc* desc = malloc(sizeof(struct filedesc));
    if (!desc) {
        set_error(UFS_ERR_NO_MEM);
        if (created) {
            if (file->refs == 0) {
                if (file->prev) file->prev->next = file->next;
                if (file->next) file->next->prev = file->prev;
                if (file_list == file) file_list = file->next;
                free(file->name);
                free(file);
            }
        }
        return -1;
    }
    
    int access_flags = flags & (UFS_READ_ONLY | UFS_WRITE_ONLY | UFS_READ_WRITE);
    if (access_flags == 0) {
        access_flags = UFS_READ_WRITE;
    }
    
    desc->file = file;
    desc->pos = 0;
    desc->flags = access_flags;
    
    file_descriptors[fd] = desc;
    file->refs++;
    file_descriptor_count++;
    
    return fd;
}

ssize_t ufs_write(int fd, const char* buf, size_t size) {
    if (fd < 0 || fd >= file_descriptor_capacity || file_descriptors[fd] == NULL) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    struct filedesc* desc = file_descriptors[fd];
    struct file* file = desc->file;
    
#if NEED_OPEN_FLAGS
    if (!(desc->flags & (UFS_WRITE_ONLY | UFS_READ_WRITE))) {
        set_error(UFS_ERR_NO_PERMISSION);
        return -1;
    }
#endif

    if (desc->pos + size > MAX_FILE_SIZE) {
        set_error(UFS_ERR_NO_MEM);
        return -1;
    }
    
    size_t new_size = desc->pos + size;
    if (new_size > file->size) {
        if (extend_file(file, new_size) < 0) {
            set_error(UFS_ERR_NO_MEM);
            return -1;
        }
    }
    
    size_t bytes_written = 0;
    size_t current_pos = desc->pos;
    
    while (bytes_written < size) {
        size_t block_index = current_pos / BLOCK_SIZE;
        size_t offset = current_pos % BLOCK_SIZE;
        
        struct block* blk = get_block(file, block_index);
        if (blk == NULL) {
            break;
        }
        
        size_t to_write = MIN(BLOCK_SIZE - offset, size - bytes_written);
        memcpy(blk->memory + offset, buf + bytes_written, to_write);
        bytes_written += to_write;
        current_pos += to_write;
    }
    
    desc->pos = current_pos;
    return bytes_written;
}

ssize_t ufs_read(int fd, char* buf, size_t size) {
    if (fd < 0 || fd >= file_descriptor_capacity || file_descriptors[fd] == NULL) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    struct filedesc* desc = file_descriptors[fd];
    struct file* file = desc->file;
    
#if NEED_OPEN_FLAGS
    if (!(desc->flags & (UFS_READ_ONLY | UFS_READ_WRITE))) {
        set_error(UFS_ERR_NO_PERMISSION);
        return -1;
    }
#endif

    if (desc->pos >= file->size) {
        return 0;
    }
    
    size_t bytes_to_read = MIN(size, file->size - desc->pos);
    size_t bytes_read = 0;
    size_t current_pos = desc->pos;
    
    while (bytes_read < bytes_to_read) {
        size_t block_index = current_pos / BLOCK_SIZE;
        size_t offset = current_pos % BLOCK_SIZE;
        
        struct block* blk = get_block(file, block_index);
        if (blk == NULL) {
            break;
        }
        
        size_t to_read = MIN(BLOCK_SIZE - offset, bytes_to_read - bytes_read);
        memcpy(buf + bytes_read, blk->memory + offset, to_read);
        bytes_read += to_read;
        current_pos += to_read;
    }
    
    desc->pos = current_pos;
    return bytes_read;
}

int ufs_close(int fd) {
    if (fd < 0 || fd >= file_descriptor_capacity || file_descriptors[fd] == NULL) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    struct filedesc* desc = file_descriptors[fd];
    struct file* file = desc->file;
    
    free(desc);
    file_descriptors[fd] = NULL;
    file_descriptor_count--;
    
    file->refs--;
    if (file->refs == 0 && file->deleted) {
        if (file->prev != NULL) {
            file->prev->next = file->next;
        } else {
            file_list = file->next;
        }
        
        if (file->next != NULL) {
            file->next->prev = file->prev;
        }
        
        struct block* blk = file->block_list;
        while (blk != NULL) {
            struct block* next = blk->next;
            free(blk->memory);
            free(blk);
            blk = next;
        }
        
        free(file->name);
        free(file);
    }
    
    return 0;
}

int ufs_delete(const char* filename) {
    struct file* file = find_file(filename);
    if (file == NULL) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }
    
    file->deleted = true;
    
    if (file->refs == 0) {
        if (file->prev != NULL) {
            file->prev->next = file->next;
        } else {
            file_list = file->next;
        }
        
        if (file->next != NULL) {
            file->next->prev = file->prev;
        }
        
        struct block* blk = file->block_list;
        while (blk != NULL) {
            struct block* next = blk->next;
            free(blk->memory);
            free(blk);
            blk = next;
        }
        
        free(file->name);
        free(file);
    }
    
    return 0;
}

#if NEED_RESIZE
int ufs_resize(int fd, size_t new_size) {
    if (fd < 0 || fd >= file_descriptor_capacity || file_descriptors[fd] == NULL) {
        set_error(UFS_ERR_NO_FILE);
        return -1;
    }

    struct filedesc* desc = file_descriptors[fd];
    struct file* file = desc->file;
    
#if NEED_OPEN_FLAGS
    if (!(desc->flags & (UFS_WRITE_ONLY | UFS_READ_WRITE))) {
        set_error(UFS_ERR_NO_PERMISSION);
        return -1;
    }
#endif

    if (new_size > MAX_FILE_SIZE) {
        set_error(UFS_ERR_NO_MEM);
        return -1;
    }
    
    size_t old_size = file->size;
    if (new_size == old_size) {
        return 0;
    }
    
    if (new_size < old_size) {
        size_t blocks_to_keep = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        struct block* blk = file->block_list;
        struct block* last_kept = NULL;
        size_t current_block_index = 0;
        
        while (blk != NULL && current_block_index < blocks_to_keep) {
            last_kept = blk;
            blk = blk->next;
            current_block_index++;
        }
        
        if (last_kept != NULL) {
            struct block* to_free = last_kept->next;
            last_kept->next = NULL;
            file->last_block = last_kept;
            
            while (to_free != NULL) {
                struct block* next = to_free->next;
                free(to_free->memory);
                free(to_free);
                to_free = next;
            }
        } else {
            struct block* to_free = file->block_list;
            file->block_list = NULL;
            file->last_block = NULL;
            
            while (to_free != NULL) {
                struct block* next = to_free->next;
                free(to_free->memory);
                free(to_free);
                to_free = next;
            }
        }
        
        file->size = new_size;

        for (int i = 0; i < file_descriptor_capacity; i++) {
            if (file_descriptors[i] != NULL && 
                file_descriptors[i]->file == file && 
                file_descriptors[i]->pos > new_size) {
                file_descriptors[i]->pos = new_size;
            }
        }
    } else {
        size_t current_blocks = (old_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        size_t needed_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        size_t blocks_to_add = needed_blocks - current_blocks;
        
        for (size_t i = 0; i < blocks_to_add; i++) {
            struct block* blk = create_block();
            if (blk == NULL) {
                set_error(UFS_ERR_NO_MEM);
                return -1;
            }
            
            if (file->last_block != NULL) {
                file->last_block->next = blk;
                blk->prev = file->last_block;
            } else {
                file->block_list = blk;
            }
            file->last_block = blk;
        }
        file->size = new_size;
    }
    
    return 0;
}
#endif

void ufs_destroy(void) {
    struct file* file = file_list;
    while (file != NULL) {
        struct file* next = file->next;
        
        struct block* blk = file->block_list;
        while (blk != NULL) {
            struct block* next_blk = blk->next;
            free(blk->memory);
            free(blk);
            blk = next_blk;
        }
        
        free(file->name);
        free(file);
        file = next;
    }
    file_list = NULL;
    
    for (int i = 0; i < file_descriptor_capacity; i++) {
        if (file_descriptors[i] != NULL) {
            free(file_descriptors[i]);
        }
    }
    free(file_descriptors);
    file_descriptors = NULL;
    
    file_descriptor_count = 0;
    file_descriptor_capacity = 0;
    set_error(UFS_ERR_NO_ERR);
}