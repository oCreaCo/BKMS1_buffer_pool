#ifndef DB_BUFFER_H_
#define DB_BUFFER_H_

#include "page.h"

#define MAX_USAGE_COUNT (5)

typedef struct buf_descriptor_t {
    int64_t table_id;
    pagenum_t page_num;
    page_t *buf_page;
//  TODO -----------------------------------------------------------------------
//  ...
//  ----------------------------------------------------------------------------
} buf_descriptor_t;

typedef struct ht_entry_t {
    buf_descriptor_t *buf_desc;
} ht_entry_t;

typedef struct hashtable_t {
    uint32_t num_ht_entries;
    ht_entry_t *ht_entries;
} hashtable_t;

typedef struct buffer_pool_t {
    uint32_t num_buf;
    hashtable_t hashtable;
//  TODO -----------------------------------------------------------------------
//  ...
//  ----------------------------------------------------------------------------
} buffer_pool_t;

void mark_buffer_dirty(buf_descriptor_t *buf_desc);
void unpin_buffer(buf_descriptor_t *buf_desc);

int64_t buffer_open_table(const char *pathname);
int init_buffer_pool(uint32_t num_ht_entries, uint32_t num_buf);
buf_descriptor_t *get_buffer(int64_t table_id, pagenum_t page_num);
buf_descriptor_t *get_buffer_of_new_page(int64_t table_id);
void free_buffer(int64_t table_id, buf_descriptor_t *free_buf);
int close_buffer_pool();

#endif // DB_BUFFER_H