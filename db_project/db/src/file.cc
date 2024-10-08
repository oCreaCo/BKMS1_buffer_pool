#include "file.h"

// table id counter
int tid_counter = 0;
table_node tables[MAX_TABLES];

// Init table nodes
int init_tables() {
    for (int i = 0; i < MAX_TABLES; i++) {
        tables[i].fd = -1;
        tables[i].table_id = -1;
    }

    tid_counter = 0;
    return 0;
}

// Search table by pathname
int64_t file_search_table_pathname(const char *pathname) {
    for (int i = 0; i < MAX_TABLES; i++) {
        if (!strcmp(pathname, tables[i].pathname))
            return tables[i].table_id;
    }

    return -1;
}

// Search fd by table_id
int file_search_table_id(int64_t table_id) {
    for (int i = 0; i < MAX_TABLES; i++) {
        if (tables[i].table_id == table_id)
            return tables[i].fd;
    }

    return -1;
}

// Insert table node into array with fd
int64_t file_insert_table(const char *pathname, int fd) {

    // Set table id with magic number
    int64_t new_id = MAGIC_NUMBER + tid_counter++;

    // Set table data into array
    strcpy(tables[tid_counter].pathname, pathname);
    tables[tid_counter].table_id = new_id;
    tables[tid_counter].fd = fd;

    // And return it
    return new_id;
}

// Open existing table file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname) {

    // First, search the table node with pathname if the table exist
    int64_t table_id = file_search_table_pathname(pathname);

    // If exist, return its id
    if (table_id > -1)
        return table_id;

    // If the table node array is filled, return -2
    if (tid_counter == MAX_TABLES)
        return -2;

    page_t *header_page = (page_t*)malloc(PAGE_SIZE);

    // Open table file
    int fd = open(pathname, O_RDWR|O_SYNC, 0644);

    // If the file exist, check the magic number
    if (fd > 0) {
        
        // Set table, get id
        table_id = file_insert_table(pathname, fd);
        file_read_page(table_id, 0, header_page);

        // If match, set table fd and return it
        if (header_page->magic_number == MAGIC_NUMBER) {
            free(header_page);
            return table_id;
        }
        // Or not, return -2 (-1 is for malloc failed)
        else {
            free(header_page);
            return -2;
        }
    }

    // Or not, create new table file
    fd = open(pathname, O_RDWR|O_CREAT|O_SYNC, 0644);
    if (fd < 0)
        return -1;

    // Set table, get id
    table_id = file_insert_table(pathname, fd);

    // Init table size (default: 10 MiB)
    uint64_t init_pages_num = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
    uint64_t init_free_pages_num = init_pages_num - 1;
    page_t *tmp_free_page = (page_t*)malloc(PAGE_SIZE);

    if (tmp_free_page == NULL) {
        free(header_page);
        return -1;
    }

    // Set and write the header page
    header_page->magic_number = MAGIC_NUMBER;
    header_page->free_page_num = init_free_pages_num;
    header_page->num_of_pages = init_pages_num;
    header_page->root_page_num = -1;
    file_write_page(table_id, 0, header_page);

    // Set and write all free pages with for-loop
    for (pagenum_t i = 1; i < init_free_pages_num;) {
        tmp_free_page->next_free_page_num = i++;
        file_write_page(table_id, i, tmp_free_page);
    }

    tmp_free_page->next_free_page_num = -1;
    file_write_page(table_id, 1, tmp_free_page);

    free(tmp_free_page);
    free(header_page);

    // Set table fd and return it
    return table_id;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id) {
    page_t *header_page = (page_t*)malloc(PAGE_SIZE);

    file_read_page(table_id, 0, header_page);

    // Read header page's free page num
    pagenum_t new_free_page_num = header_page->free_page_num;
    page_t *tmp_page = (page_t*)malloc(PAGE_SIZE);

    // If there is a free page, get it and fix the list order
    if (new_free_page_num != -1) {
        file_read_page(table_id, header_page->free_page_num, tmp_page);
        header_page->free_page_num = tmp_page->next_free_page_num;
        file_write_page(table_id, 0, header_page);
    }
    // Or not, double the entire database and return new one;
    else {
        tmp_page->next_free_page_num = -1;
        file_write_page(table_id, header_page->num_of_pages, tmp_page);

        // Double the database
        uint64_t num_of_pages = 2 * header_page->num_of_pages;
        new_free_page_num = num_of_pages - 1;

        for (pagenum_t i = header_page->num_of_pages; i < num_of_pages - 2;) {
            tmp_page->next_free_page_num = i++;
            file_write_page(table_id, i, tmp_page);
        }

        header_page->free_page_num = num_of_pages - 2;
        header_page->num_of_pages = num_of_pages;
        file_write_page(table_id, 0, header_page);
    }

    free(header_page);
    free(tmp_page);

    return new_free_page_num;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum) {
    page_t *header_page = (page_t*)malloc(PAGE_SIZE);
    page_t *tmp_page = (page_t*)malloc(PAGE_SIZE);

    file_read_page(table_id, 0, header_page);

    // Set the page to free page and write it
    tmp_page->next_free_page_num = header_page->free_page_num;
    file_write_page(table_id, pagenum, tmp_page);
    free(tmp_page);

    // And adjust that to header page
    header_page->free_page_num = pagenum;
    file_write_page(table_id, 0, header_page);
    free(header_page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, struct page_t* dest) {
    pread(file_search_table_id(table_id), dest, PAGE_SIZE, PAGE_SIZE * pagenum);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const struct page_t* src) {
    pwrite(file_search_table_id(table_id), src, PAGE_SIZE, PAGE_SIZE * pagenum);
}

// Close the table file
void file_close_table_files() {
    for (int i = 0; i < MAX_TABLES; i++) {
        if (tables[i].fd != 0) {
            close(tables[i].fd);
            tables[i].fd = -1;
            tables[i].table_id = -1;
        }
    }

    tid_counter = 0;
}
