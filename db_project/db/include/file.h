#ifndef DB_FILE_H_
#define DB_FILE_H_

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include "page.h"

#define MAX_TABLES 20

typedef struct table_node {
    char pathname[128];
    int64_t table_id;
    int fd;
} table_node;

// Init table nodes
int init_tables();

// Open existing table file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, struct page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const struct page_t* src);

// Close the table file
void file_close_table_files();

#endif  // DB_FILE_H_
