#ifndef DB_PAGE_H_
#define DB_PAGE_H_

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>

#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB
#define MAGIC_NUMBER 2024
#define HEADER_SIZE 128
#define SLOT_SIZE 12
#define DATA_SIZE (PAGE_SIZE - HEADER_SIZE)
#define INTERNAL_ORDER 249

typedef uint64_t pagenum_t;
typedef int64_t db_key_t;
typedef char byte;

typedef struct kp_pair {
    db_key_t key;
    pagenum_t page_num;
} kp_pair;

typedef struct slot_t {
    db_key_t key;
    uint16_t size;
    uint16_t offset;
} slot_t;

struct page_t {
    union {
        struct { // Header Page
            uint64_t magic_number;
            pagenum_t free_page_num;
            uint64_t num_of_pages;
            pagenum_t root_page_num;
        };
        struct { // Free Page
            pagenum_t next_free_page_num;
        };
        struct { // Node Page
            // Node Header
            pagenum_t parent_page_num;
            int32_t is_leaf;
            int32_t num_of_keys;
            union {
                struct { // Internal Page
                    // Internal Header
                    byte reserved_internal[104];
                    pagenum_t most_left_page_num;

                    // Internal Data
                    kp_pair pairs[INTERNAL_ORDER - 1];
                };
                struct { // Leaf Page
                    // Leaf Header
                    byte reserved_leaf[96];
                    uint64_t amount_of_free_space;
                    pagenum_t right_sibling_page_num;

                    // Leaf Data
                    byte data[DATA_SIZE];
                };
            };
        };
        byte space[PAGE_SIZE];
    };
};

typedef struct page_t page_t;

#endif // DB_PAGE_H