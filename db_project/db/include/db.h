#ifndef DB_DB_H_
#define DB_DB_H_

#include <stdio.h>
#include <cstring>
#include <cstdint>
#include <vector>

#include "buffer.h"

// Min, Max of value size
#define MIN_VALUE_SIZE 50
#define MAX_VALUE_SIZE 112

// Threshold of deletion
#define THRESHOLD 2500


// Insertion

int insert_into_leaf(int64_t table_id, buf_descriptor_t *leaf_buf,
                     db_key_t key, const char* value, uint16_t val_size);

int insert_into_leaf_after_splitting(int64_t table_id, buf_descriptor_t* leaf_buf,
                                     db_key_t key, const char* value, uint16_t val_size);

int insert_into_node(int64_t table_id, buf_descriptor_t* parent_buf,
                     int right_index, uint64_t key, pagenum_t right_num);

int insert_into_node_after_splitting(int64_t table_id, buf_descriptor_t *parent_buf,
                                     int right_index, uint64_t key, pagenum_t right_num);

int insert_into_parent(int64_t table_id, buf_descriptor_t *left_buf, uint64_t key,
                       buf_descriptor_t *right_buf);

int insert_into_new_root(int64_t table_id, buf_descriptor_t *left_buf, uint64_t key,
                         buf_descriptor_t *right_buf);

int start_new_tree(int64_t table_id, db_key_t key, const char *value, uint16_t val_size);


// Deletion

int remove_entry_from_page(int64_t table_id, buf_descriptor_t* buf, uint64_t key);

int adjust_root(int64_t table_id, buf_descriptor_t* root_buf);

int coalesce_nodes(int64_t table_id, buf_descriptor_t *buf,
                   buf_descriptor_t *neighbor_buf, buf_descriptor_t *parent_buf,
                   int neighbor_index, int k_prime);

int redistribute_nodes(int64_t table_id, buf_descriptor_t *buf,
                       buf_descriptor_t *neighbor_buf, buf_descriptor_t *parent_buf,
                       int neighbor_index, int k_prime_index, int k_prime);

int delete_entry(int64_t table_id, buf_descriptor_t* buf, uint64_t key);


// Index manager APIs

// Open an existing database file or create one if not exist.
int64_t open_table(const char *pathname);

// Insert a record to the given table.
int db_insert(int64_t table_id, int64_t key, const char *value, uint16_t val_size);

// Find a record with the matching key from the given table.
int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size);

// Delete a record with the matching key from the given table.
int db_delete(int64_t table_id, int64_t key);

// Find records with a key betwen the range: begin_key <= key <= end_key
int db_scan(int64_t table_id, int64_t begin_key, int64_t end_key,
            std::vector<int64_t> *keys, std::vector<char*> *values,
            std::vector<uint16_t> *val_sizes);

// Initialize the database system.
int init_db(uint32_t num_ht_entries, uint32_t num_buf);

// Shutdown the databasee system.
int shutdown_db();

#endif  // DB_DB_H_