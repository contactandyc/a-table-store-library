// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_storage.h"
#include "a-memory-library/aml_alloc.h"
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

typedef struct { int fd; } posix_file_t;

static void* posix_open_reader(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return NULL;
    posix_file_t *f = aml_malloc(sizeof(posix_file_t)); // [Phase 4A Fix] Standardize allocators
    if (!f) { close(fd); return NULL; }
    f->fd = fd;
    return f;
}

static void* posix_open_writer(const char *path) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return NULL;
    posix_file_t *f = aml_malloc(sizeof(posix_file_t));
    if (!f) { close(fd); return NULL; }
    f->fd = fd;
    return f;
}

static void* posix_open_appender(const char *path) {
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return NULL;
    posix_file_t *f = aml_malloc(sizeof(posix_file_t));
    if (!f) { close(fd); return NULL; }
    f->fd = fd;
    return f;
}

static ssize_t posix_pread(void *ctx, void *buf, size_t size, uint64_t offset) {
    int fd = ((posix_file_t*)ctx)->fd;
    size_t total_read = 0;
    char *ptr = (char *)buf;

    while (total_read < size) {
        ssize_t ret = pread(fd, ptr + total_read, size - total_read, offset + total_read);
        if (ret < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            return -1; // [Phase 4A Fix] Return explicit IO error
        }
        if (ret == 0) break; // EOF
        total_read += ret;
    }
    return total_read;
}

static size_t posix_append(void *ctx, const void *buf, size_t size) {
    int fd = ((posix_file_t*)ctx)->fd;
    size_t total_written = 0;
    const char *ptr = (const char *)buf;

    while (total_written < size) {
        ssize_t ret = write(fd, ptr + total_written, size - total_written);
        if (ret < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            break;
        }
        total_written += ret;
    }
    return total_written;
}

static int posix_fsync(void *ctx) {
    int fd = ((posix_file_t*)ctx)->fd;
    return fsync(fd);
}

static void posix_close(void *ctx) {
    if (ctx) {
        close(((posix_file_t*)ctx)->fd);
        aml_free(ctx);
    }
}

static bool posix_delete(const char *path) {
    return unlink(path) == 0;
}

lsm_storage_backend_t local_posix_backend = {
    .open_reader = posix_open_reader,
    .open_writer = posix_open_writer,
    .open_appender = posix_open_appender,
    .pread = posix_pread,
    .append = posix_append,
    .fsync_file = posix_fsync,
    .close_reader = posix_close,
    .close_writer = posix_close,
    .delete_file = posix_delete
};
