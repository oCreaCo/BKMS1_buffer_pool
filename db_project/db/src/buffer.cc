#include "buffer.h"
#include "file.h"

// For stats
int64_t stat_get_buffer;

buffer_pool_t buffer_pool;

void inline flush_buffer(buf_descriptor_t *buf_desc) {
    file_write_page(buf_desc->table_id, buf_desc->page_num, buf_desc->buf_page);
}

int64_t buffer_open_table(const char *pathname) {
    return file_open_table_file(pathname);
}

void inline mark_buffer_dirty(buf_descriptor_t *buf_desc) {
//  TODO -----------------------------------------------------------------------
    flush_buffer(buf_desc);
//  ----------------------------------------------------------------------------
}

void inline pin_buffer(buf_descriptor_t *buf_desc) {
//  TODO -----------------------------------------------------------------------
//  ...
//  ----------------------------------------------------------------------------
}

void inline unpin_buffer(buf_descriptor_t *buf_desc) {
//  TODO -----------------------------------------------------------------------
    free(buf_desc->buf_page);
    free(buf_desc);
//  ----------------------------------------------------------------------------
}

/**
 * @brief Initialize the hashtable of the buffer pool.
 * 
 * @param num_ht_entries The number of hashtable entries
 * 
 * @details Initialize the hashtable using num_ht_entries. 
 */
void init_hashtable(uint32_t num_ht_entries) {

//  TODO -----------------------------------------------------------------------
//  ...
//  ----------------------------------------------------------------------------

    buffer_pool.hashtable.num_ht_entries = num_ht_entries;
}

/**
 * @brief Initialize the buffer pool.
 * 
 * @param num_ht_entries The number of hashtable entries
 * @param num_buf The number of buffer (descriptor and page)
 * @retval 0: successful
 * @retval others: failed
 * 
 * @details The num_buf must be greater or equal than 4 
 * (The splitting, deleting operation pins 3 page at once) + (header page)
 */
int init_buffer_pool(uint32_t num_ht_entries, uint32_t num_buf) {
    buf_descriptor_t *buf;

    if (num_buf < 4)
        return 1;

    if (init_tables())
        return 1;

    init_hashtable(num_ht_entries);

//  TODO -----------------------------------------------------------------------
//  ...
//  ----------------------------------------------------------------------------

    buffer_pool.num_buf = num_buf;

    init_buffer_stat();

    return 0;
}

// macros for hashtable
//  TODO -----------------------------------------------------------------------

/**
 * @brief 
 * If necessary, modify these functions or implement an additional functions
 * of your own.
 */

#define hash(table_id, page_num) \
    ((((uint64_t)table_id) * 100000 + page_num)%(buffer_pool.hashtable.num_ht_entries))
#define get_ht_entry(table_id, page_num) \
    (&(buffer_pool.hashtable.ht_entries[hash(table_id, page_num)]))

//  or
//
//  ht_entry_t *get_ht_entry(int64_t table_id, pagenum_t page_num) {
//      ...
//  }

//  ----------------------------------------------------------------------------

/**
 * @brief Look up the buffer(page) in hashtable.
 * 
 * @return The memory address of the found buffer descriptor.
 */
inline buf_descriptor_t *hashtable_lookup(int64_t table_id, pagenum_t page_num) {
    ht_entry_t *ht_entry;
    buf_descriptor_t *buf_desc;

//  TODO -----------------------------------------------------------------------
    buf_desc = NULL;
//  ----------------------------------------------------------------------------

    return buf_desc;
}

/**
 * @brief Insert a new buffer(page) into the hashtable.
 * 
 * @details Assume this page is not in the hashtable.
 */
inline void hashtable_insert(buf_descriptor_t *buf_desc) {
    ht_entry_t *ht_entry;

//  TODO -----------------------------------------------------------------------
//  ...
//  ----------------------------------------------------------------------------
}

/**
 * @brief Delete the buffer(page) from the hashtable.
 * 
 * @details Assume this page is in the hashtable.
 */
inline void hashtable_delete(buf_descriptor_t *buf_desc) {
    ht_entry_t *ht_entry;

//  TODO -----------------------------------------------------------------------
//  ...
//  ----------------------------------------------------------------------------
}

/**
 * @brief Get the victim buffer of eviction.
 * 
 * @return The memory address of the victim buffer.
 * 
 * @details
 * This function selects a victim buffer for eviction, following the buffer
 * replacement policy. (clock sweep algorithm in this code)
 * 
 * This first checks if there is an unused buffer in the buffer pool's freelist
 * and returns it if there is. Otherwise, selects and returns an appropriate
 * victim buffer according to the replacement policy.
 * 
 * Return NULL if all buffers in the buffer pool are pinned.
 */
buf_descriptor_t *get_victim_buffer() {
    buf_descriptor_t *buf_desc;

//  TODO -----------------------------------------------------------------------
    buf_desc = (buf_descriptor_t*)malloc(sizeof(buf_descriptor_t));
    buf_desc->buf_page = (page_t*)malloc(PAGE_SIZE);

    if (buf_desc != NULL)
        return buf_desc;
//  ----------------------------------------------------------------------------

    return NULL;
}

/**
 * @brief Get the buffer of the requested page.
 * 
 * @return The memory address of the buffer descriptor.
 * 
 * @details
 * This function receives a request for a page with table_id and page_num, and
 * returns a buffer containing that page. This first checks if the requested
 * page exists in the buffer pool using a hashtable. If the page exists in the
 * buffer pool, it returns the buffer simply. Otherwise, the other buffer is
 * reserved through the following steps:
 * 
 * 1. Get a new buffer by calling get_victim_buffer().
 * 
 * 2. Flush the buffer by calling flush_buffer() if the buffer is dirty.
 * 
 * 3. Delete the buffer from the hashtable if necessary.
 * 
 * 4. Set the table_id and page_num of the buffer and read page to the buffer by
 * calling file_read_page();
 * 
 * 5. Insert the buffer to the hashtable.
 * 
 * During this process, the usage count must not exceed MAX_USAGE_COUNT, and
 * buf_desc must increment the reference count by calling pin_buffer() before
 * being returned.
 */
buf_descriptor_t *get_buffer(int64_t table_id, pagenum_t page_num) {
    buf_descriptor_t *buf_desc;

    stat_get_buffer++;

//  TODO -----------------------------------------------------------------------
    buf_desc = get_victim_buffer();
    pin_buffer(buf_desc);

    buf_desc->table_id = table_id;
    buf_desc->page_num = page_num;
    file_read_page(table_id, page_num, buf_desc->buf_page);
//  ----------------------------------------------------------------------------

    return buf_desc;
}

buf_descriptor_t *get_buffer_of_new_page(int64_t table_id) {
    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    page_t *header_page = header_buf->buf_page;
    pagenum_t new_page_num = header_page->free_page_num;
    buf_descriptor_t *buf_desc;

    // Ger header page and free page number from it.

    // If there is any free page, get the page.
    if (new_page_num != -1) {
        buf_desc = get_buffer(table_id, new_page_num);
        header_page->free_page_num = buf_desc->buf_page->next_free_page_num;
    }
    // Or not, double the database.
    else {
        // Double the database.
        uint64_t num_of_pages = 2 * header_page->num_of_pages;
        page_t *tmp_page = (page_t*)malloc(PAGE_SIZE);

        // Write new free page number.
        tmp_page->next_free_page_num = -1;
        file_write_page(table_id, header_page->num_of_pages, tmp_page);

        // Write free pages.
        for (pagenum_t i = header_page->num_of_pages; i < num_of_pages - 2;) {
            tmp_page->next_free_page_num = i++;
            file_write_page(table_id, i, tmp_page);
        }

        // Set header page.
        header_page->free_page_num = num_of_pages - 2;
        header_page->num_of_pages = num_of_pages;

        free(tmp_page);

        // Get new buffer.
        new_page_num = num_of_pages - 1;
        buf_desc = get_buffer(table_id, new_page_num);
    }

    mark_buffer_dirty(header_buf);
    unpin_buffer(header_buf);
    
    return buf_desc;
}

void free_page(int64_t table_id, buf_descriptor_t *buf) {
    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    page_t *header_page = header_buf->buf_page;
    page_t *buf_page = buf->buf_page;

    // Add this page to the free page list.
    buf_page->next_free_page_num = header_page->free_page_num;
    header_page->free_page_num = buf->page_num;

    mark_buffer_dirty(header_buf);
    mark_buffer_dirty(buf);
    unpin_buffer(header_buf);
}

int close_buffer_pool() {
    buf_descriptor_t *buf_desc;
    int ret = 0;

//  TODO -----------------------------------------------------------------------
//  ...
//  ----------------------------------------------------------------------------

    file_close_table_files();

    return ret;
}

void init_buffer_stat() {
    stat_get_buffer = 0;
    stat_read_page = 0;
    stat_write_page = 0;
}

int64_t get_buffer_hit_ratio() {
    return (stat_get_buffer - stat_read_page) * 100 /
           (stat_get_buffer);
}

std::string get_buffer_stat() {
    int64_t hit_ratio = get_buffer_hit_ratio();

    return string_format("get_buffer() count: %ld, file_read_page() count: %ld, file_write_page() count: %ld, buffer hit ratio: %ld%%",
                          stat_get_buffer, stat_read_page, stat_write_page, hit_ratio);
}

void print_buffer_stat() {
    std::cerr << get_buffer_stat() << std::endl;
}