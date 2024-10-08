#include "db.h"

// macro for getting slot
#define get_slot(data, idx) \
    ((slot_t*)((data) + (idx) * SLOT_SIZE))

/* Traces the path from the root to a leaf, searching
 * by key.
 * Returns the leaf page containing the given key.
 */
buf_descriptor_t *find_leaf(int64_t table_id, db_key_t key,
                            pagenum_t* p_num_ref = NULL) {
    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    pagenum_t p_num = header_buf->buf_page->root_page_num;
    unpin_buffer(header_buf);

    if (p_num == -1)
        return NULL;

    // Start from root page.
    buf_descriptor_t *tmp_buf = get_buffer(table_id, p_num);
    page_t *tmp_page = tmp_buf->buf_page;
    int p_index;
    
    // Iterate until the leaf page is reached.
    while (!tmp_page->is_leaf) {
        p_index = -1;

        // Find offset.
        while (p_index < (tmp_page->num_of_keys - 1)) {
            if (tmp_page->pairs[p_index + 1].key <= key)
                p_index++;
            else
                break;
        }

        // Most left page or not.
        if (p_index >= 0)
            p_num = tmp_page->pairs[p_index].page_num;
        else
            p_num = tmp_page->most_left_page_num;

        // Release current internal page and get its child page.
        unpin_buffer(tmp_buf);
        tmp_buf = get_buffer(table_id, p_num);
        tmp_page =tmp_buf->buf_page;
    }

    if (p_num_ref != NULL)
        *p_num_ref = p_num;

    return tmp_buf;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

/* Creates a new internal page. */
buf_descriptor_t *make_node(int64_t table_id) {
    buf_descriptor_t *new_buf = get_buffer_of_new_page(table_id);
    page_t *new_page = new_buf->buf_page;

    new_page->parent_page_num = -1;
    new_page->is_leaf = 0;
    new_page->num_of_keys = 0;
    new_page->most_left_page_num = -1;

    return new_buf;
}

/* Creates a new leaf page. */
buf_descriptor_t *make_leaf(int64_t table_id) {
    buf_descriptor_t *new_buf = make_node(table_id);
    page_t *new_page = new_buf->buf_page;

    new_page->is_leaf = 1;
    new_page->amount_of_free_space = DATA_SIZE;
    new_page->right_sibling_page_num = -1;

    return new_buf;
}

/* Helper function used in insert_into_parent
 * to find the index of the parent's page num position 
 * to the right of the key to be inserted.
 */
int get_right_index(page_t* parent, db_key_t key) {

    int right_index = 0;
    while (right_index < parent->num_of_keys && parent->pairs[right_index].key < key)
        right_index++;

    return right_index;
}

/* Inserts a new key and value into a leaf.
 * Returns the altered leaf.
 */
int insert_into_leaf(int64_t table_id, buf_descriptor_t *leaf_buf,
                     db_key_t key, const char* value, uint16_t val_size) {
    int i, j;
    page_t *leaf_page = leaf_buf->buf_page;
    slot_t *slot;
    slot_t *next_slot;
    uint16_t temp_offset;

    // First, find the insertion point of this leaf page.
    for (i = 0; i < leaf_page->num_of_keys; i++) {
        slot = get_slot(leaf_page->data, i);

        if (slot->key >= key)
            break;
    }

    /* Second, moves all pages to the right of the insertion point one index to
     * the right.
     */
    for (j = leaf_page->num_of_keys; j > i; j--) {
        slot = get_slot(leaf_page->data, j - 1);
        next_slot = get_slot(leaf_page->data, j);

        temp_offset = slot->offset - val_size;

        // Set the next slot.
        next_slot->key = slot->key;
        next_slot->size = slot->size;
        next_slot->offset = temp_offset;

        // Copy record.
        memcpy((char*)leaf_page + temp_offset,
               (char*)leaf_page + slot->offset, slot->size);
    }
    
    // Decide offset of new record by the insertion point.
    if (i > 0)
        temp_offset = get_slot(leaf_page->data, i - 1)->offset - val_size;
    else
        temp_offset = PAGE_SIZE - val_size;

    // Get slot of new record.
    slot = get_slot(leaf_page->data, i);

    // Set metadata.
    slot->key = key;
    slot->size = val_size;
    slot->offset = temp_offset;

    // Write record.
    memcpy((char*)leaf_page + temp_offset, value, val_size);
    leaf_page->num_of_keys++;
    leaf_page->amount_of_free_space -= (SLOT_SIZE + val_size);

    mark_buffer_dirty(leaf_buf);
    unpin_buffer(leaf_buf);
    
    return 0;
}

/* Inserts a new key and value
 * to a new record into a leaf so as to exceed
 * the page size, causing the leaf to be split
 * in half.
 */
int insert_into_leaf_after_splitting(int64_t table_id, buf_descriptor_t* leaf_buf,
                                     db_key_t key, const char* value, uint16_t val_size) {
    
    slot_t *slot;
    page_t *leaf_page = leaf_buf->buf_page;
    int insertion_index = -1, split = -1, i, j;
    db_key_t new_key;
    char data_buffer[DATA_SIZE];
    uint16_t size;

    // Back up data of original page
    memcpy(data_buffer, leaf_page->data, DATA_SIZE);

    // First, find the insertion point of this leaf page.
    for (insertion_index = 0; insertion_index < leaf_page->num_of_keys; insertion_index++) {
        slot = get_slot(data_buffer, insertion_index);

        if (slot->key > key)
            break;
    }

    // Second, get the split point.
    size = 0;
    for (i = 0, j = 0; i < leaf_page->num_of_keys; i++, j++) {
        if (i == insertion_index) {
            size += (SLOT_SIZE + val_size);

            if (size > (DATA_SIZE / 2))
                break;
            j++;
        }
        
        slot = get_slot(data_buffer, i);
        size += (SLOT_SIZE + slot->size);

        if (size > (DATA_SIZE / 2))
            break;
    }
    split = j;

    // making new leaf
    buf_descriptor_t* new_leaf_buf = make_leaf(table_id);
    page_t *new_leaf_page = new_leaf_buf->buf_page;
    uint64_t temp_offset = PAGE_SIZE;
    slot_t *temp_slot;

    /* Move records within the range to the new leaf page.
     * (split point <= key < insertion point)
     */
    for (i = split, j = 0; i < leaf_page->num_of_keys && i < insertion_index; i++, j++) {
        slot = get_slot(data_buffer, i);
        temp_slot = get_slot(new_leaf_page->data, j);

        size = slot->size;
        temp_offset -= size;
        
        temp_slot->key = slot->key;
        temp_slot->size = size;
        temp_slot->offset = temp_offset;

        memcpy((char*)new_leaf_page + temp_offset, data_buffer + slot->offset - HEADER_SIZE, size);
        new_leaf_page->amount_of_free_space -= (SLOT_SIZE + size);
    }

    // If the insertion point is in the new leaf page, insert the record.
    if (i == insertion_index) {
        temp_slot = get_slot(new_leaf_page->data, j);

        temp_offset -= val_size;
        
        temp_slot->key = key;
        temp_slot->size = val_size;
        temp_slot->offset = temp_offset;

        memcpy((char*)new_leaf_page + temp_offset, value, val_size);
        new_leaf_page->amount_of_free_space -= (SLOT_SIZE + val_size);
        j++;
    } else {
        i--; // For new record
    }

    /* Move the remaining records.
     * (insertion point < key < num_of_keys)
     */
    for (; i < leaf_page->num_of_keys; i++, j++) {
        slot = get_slot(data_buffer, i);
        temp_slot = get_slot(new_leaf_page->data, j);

        size = slot->size;
        temp_offset -= size;
        
        temp_slot->key = slot->key;
        temp_slot->size = size;
        temp_slot->offset = temp_offset;

        memcpy((char*)new_leaf_page + temp_offset, data_buffer + slot->offset - HEADER_SIZE, size);
        new_leaf_page->amount_of_free_space -= (SLOT_SIZE + size);
    }

    new_leaf_page->num_of_keys = leaf_page->num_of_keys - split + 1;

    // setting original leaf

    // Re-calculate the amount of free space of original leaf page.
    leaf_page->amount_of_free_space = DATA_SIZE;
    for (i = 0; i < split && i < insertion_index; i++) {
        slot = get_slot(data_buffer, i);
        leaf_page->amount_of_free_space -= (SLOT_SIZE + slot->size);
    }

    // If the insertion point is in the original leaf page, insert the record.
    if (i == insertion_index) {
        temp_slot = get_slot(leaf_page->data, i);

        if (i > 0)
            temp_offset = get_slot(leaf_page->data, i - 1)->offset - val_size;
        else
            temp_offset = PAGE_SIZE - val_size;
        
        temp_slot->key = key;
        temp_slot->size = val_size;
        temp_slot->offset = temp_offset;

        memcpy((char*)leaf_page + temp_offset, value, val_size);
        leaf_page->amount_of_free_space -= (SLOT_SIZE + val_size);
    }

    // Compact the remaining records.
    for (; i < split; i++) {
        slot = get_slot(data_buffer, i);
        temp_slot = get_slot(leaf_page->data, i + 1);

        size = slot->size;
        temp_offset = slot->offset - val_size;
        
        temp_slot->key = slot->key;
        temp_slot->size = size;
        temp_slot->offset = temp_offset;

        memcpy((char*)leaf_page + temp_offset, data_buffer + slot->offset - HEADER_SIZE, size);
        leaf_page->amount_of_free_space -= (SLOT_SIZE + size);
    }

    leaf_page->num_of_keys = split;

    new_leaf_page->right_sibling_page_num = leaf_page->right_sibling_page_num;
    leaf_page->right_sibling_page_num = new_leaf_buf->page_num;
    
    new_leaf_page->parent_page_num = leaf_page->parent_page_num;
    new_key = get_slot(new_leaf_page->data, 0)->key;

    return insert_into_parent(table_id, leaf_buf, new_key, new_leaf_buf);
}

/* Inserts a new key and page num
 * into an internal page into which these can fit
 * without violating the B+ tree properties.
 */
int insert_into_node(int64_t table_id, buf_descriptor_t *internal_buf,
                     int right_index, uint64_t key, pagenum_t right_num) {
    page_t *parent_page = internal_buf->buf_page;

    for (int i = parent_page->num_of_keys; i > right_index; i--)
        parent_page->pairs[i] = parent_page->pairs[i - 1];

    parent_page->pairs[right_index].key = key;
    parent_page->pairs[right_index].page_num = right_num;
    parent_page->num_of_keys++;

    mark_buffer_dirty(internal_buf);
    unpin_buffer(internal_buf);

    return 0;
}

/* Inserts a new key and page num
 * into an internal page, causing the page's size to exceed
 * the order, and causing the page to split into two.
 */
int insert_into_node_after_splitting(int64_t table_id, buf_descriptor_t *internal_buf,
                                     int right_index, uint64_t key, pagenum_t right_num) {
    page_t* internal_page = internal_buf->buf_page;

    int i, j, split;
    uint64_t k_prime;
    kp_pair temp_nodes[INTERNAL_ORDER+5];

    /* First create a temporary set of key - page num pairs
     * to hold everything in order, including
     * the new key and page num, inserted in their
     * correct places. 
     * Then create a new page and copy half of the 
     * keys and page nums to the old page and
     * the other half to the new.
     */

    for (i = 0, j = 0; i < INTERNAL_ORDER - 1; i++, j++) {
        if (j == right_index)
            j++;

        temp_nodes[j] = internal_page->pairs[i];
    }

    temp_nodes[right_index].key = key;
    temp_nodes[right_index].page_num = right_num;

    /* Create the new page and copy
     * half the keys and pointers to the
     * old and half to the new.
     */  
    split = cut(INTERNAL_ORDER) - 1;
    
    buf_descriptor_t *new_internal_buf = make_node(table_id);
    pagenum_t new_internal_page_num = new_internal_buf->page_num;
    page_t *new_internal_page = new_internal_buf->buf_page;

    internal_page->num_of_keys = 0;

    // Left, original internal page.
    for (i = 0; i < split; i++) {
        internal_page->pairs[i] = temp_nodes[i];
        internal_page->num_of_keys++;
    }
    
    k_prime = temp_nodes[split].key;
    new_internal_page->most_left_page_num = temp_nodes[split].page_num;
    new_internal_page->parent_page_num = internal_page->parent_page_num;

    // Right, new internal page.
    for (++i, j = 0; i < INTERNAL_ORDER; i++, j++) {
        new_internal_page->pairs[j] = temp_nodes[i];
        new_internal_page->num_of_keys++;
    }

    pagenum_t child_num = new_internal_page->most_left_page_num;
    buf_descriptor_t *child_buf = get_buffer(table_id, child_num);

    child_buf->buf_page->parent_page_num = new_internal_page_num;

    mark_buffer_dirty(child_buf);
    unpin_buffer(child_buf);

    // Set the parent number of child pages.
    for (i = 0; i < new_internal_page->num_of_keys; i++) {
        child_num = new_internal_page->pairs[i].page_num;
        child_buf = get_buffer(table_id, child_num);
        child_buf->buf_page->parent_page_num = new_internal_page_num;
        mark_buffer_dirty(child_buf);
        unpin_buffer(child_buf);
    }

    /* Insert a new key into the parent of the two
     * pages resulting from the split, with
     * the old page to the left and the new to the right.
     */

    return insert_into_parent(table_id, internal_buf, k_prime, new_internal_buf);
}

/* Inserts a new page (leaf or internal) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
int insert_into_parent(int64_t table_id, buf_descriptor_t *left_buf, uint64_t key,
                       buf_descriptor_t *right_buf) {
    page_t *left_page = left_buf->buf_page;

    int right_index;
    pagenum_t parent_num = left_page->parent_page_num;
    pagenum_t right_num = right_buf->page_num;

    /* Case: new root. */

    if (parent_num == -1)
        return insert_into_new_root(table_id, left_buf, key, right_buf);

    mark_buffer_dirty(left_buf);
    mark_buffer_dirty(right_buf);
    unpin_buffer(left_buf);
    unpin_buffer(right_buf);

    buf_descriptor_t *parent_buf = get_buffer(table_id, parent_num);
    page_t *parent = parent_buf->buf_page;

    right_index = get_right_index(parent, key);


    /* Simple case: the new key fits into the node. 
     */

    if (parent->num_of_keys < INTERNAL_ORDER - 1)
        return insert_into_node(table_id, parent_buf, right_index, key, right_num);

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */

    return insert_into_node_after_splitting(table_id, parent_buf, right_index, key, right_num);
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
int insert_into_new_root(int64_t table_id, buf_descriptor_t *left_buf, uint64_t key,
                         buf_descriptor_t *right_buf) {
    buf_descriptor_t *root_buf = make_node(table_id);
    page_t* root_page = root_buf->buf_page;

    root_page->most_left_page_num = left_buf->page_num;
    root_page->pairs[0].key = key;
    root_page->pairs[0].page_num = right_buf->page_num;
    root_page->num_of_keys++;
    root_page->parent_page_num = -1;
    left_buf->buf_page->parent_page_num = root_buf->page_num;
    right_buf->buf_page->parent_page_num = root_buf->page_num;

    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    header_buf->buf_page->root_page_num = root_buf->page_num;

    mark_buffer_dirty(left_buf);
    mark_buffer_dirty(right_buf);
    mark_buffer_dirty(root_buf);
    mark_buffer_dirty(header_buf);
    unpin_buffer(left_buf);
    unpin_buffer(right_buf);
    unpin_buffer(root_buf);
    unpin_buffer(header_buf);

    return 0;
}

int start_new_tree(int64_t table_id, db_key_t key, const char *value, uint16_t val_size) {
    buf_descriptor_t *root_buf = make_leaf(table_id);
    page_t *root_page = root_buf->buf_page;

    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    page_t *header_page = header_buf->buf_page;
    header_page->root_page_num = root_buf->page_num;
    
    slot_t *slot = get_slot(root_page->data, 0);
    uint16_t new_offset = PAGE_SIZE - val_size;

    slot->key = key;
    slot->size = val_size;
    slot->offset = new_offset;

    memcpy((char*)root_page + new_offset, value, val_size);

    root_page->parent_page_num = -1;
    root_page->num_of_keys++;
    root_page->amount_of_free_space -= (SLOT_SIZE + val_size);

    mark_buffer_dirty(root_buf);
    mark_buffer_dirty(header_buf);
    unpin_buffer(root_buf);
    unpin_buffer(header_buf);

    return 0;
}


/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index(page_t* parent, pagenum_t p_num)
{
    if (parent->most_left_page_num == p_num)
        return -1;

    for (int i = 0; i < parent->num_of_keys; i++) {
        if (parent->pairs[i].page_num == p_num)
            return i;
    }

    // Error state.
    printf("Search for nonexistent index of page in parent.\n");
    exit(EXIT_FAILURE);
}

int remove_entry_from_page(int64_t table_id, buf_descriptor_t* buf, uint64_t key) {
    page_t *page = buf->buf_page;

    int i;

    // Remove the key and shift other keys accordingly.
    i = 0;

    if (!page->is_leaf) {
        // Find the deletion point.
        while (page->pairs[i].key != key)
            i++;

        // Shift the remaining key-pointer pairs. 
        for (i++; i < page->num_of_keys; i++)
            page->pairs[i - 1] = page->pairs[i];
    } else {
        uint16_t val_size;
        uint64_t temp_offset;

        // Find the deletion point
        slot_t *slot;
        for (; i < page->num_of_keys; i++) {
            slot = get_slot(page->data, i);

            if (slot->key == key) {
                val_size = slot->size;
                break;
            }
        }

        // Shift the remaining slots and records.
        slot_t *next_slot;
        for (++i; i < page->num_of_keys; i++) {
            slot = get_slot(page->data, i - 1);
            next_slot = get_slot(page->data, i);

            temp_offset = next_slot->offset + val_size;

            slot->key = next_slot->key;
            slot->size = next_slot->size;
            slot->offset = temp_offset;

            memcpy((char*)page + temp_offset,
                   (char*)page + next_slot->offset, next_slot->size);
        }

        page->amount_of_free_space += (SLOT_SIZE + val_size);
    }

    // One key fewer.
    (page->num_of_keys)--;

    mark_buffer_dirty(buf);

    return 0;
}

int adjust_root(int64_t table_id, buf_descriptor_t* root_buf) {
    page_t *root_page = root_buf->buf_page;

    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (root_page->num_of_keys > 0) {
        unpin_buffer(root_buf);
        return 0;
    }

    /* Case: empty root. 
     */

    free_buffer(table_id, root_buf);

    // If it has a child, promote 
    // the first (only) child
    // as the new root.

    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    page_t *header_page = header_buf->buf_page;

    if (!root_page->is_leaf) {
        header_page->root_page_num = root_page->most_left_page_num;
        unpin_buffer(root_buf);
        root_buf = get_buffer(table_id, header_page->root_page_num);
        root_buf->buf_page->parent_page_num = -1;
        mark_buffer_dirty(root_buf);
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else
        header_page->root_page_num = -1;

    mark_buffer_dirty(header_buf);
    unpin_buffer(header_buf);
    unpin_buffer(root_buf);

    return 0;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_nodes(int64_t table_id, buf_descriptor_t *buf,
                   buf_descriptor_t *neighbor_buf, buf_descriptor_t *parent_buf,
                   int neighbor_index, int k_prime) {

    int i, j, n_end, insertion_index;

    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */

    if (neighbor_index == -1) {
        buf_descriptor_t *tmp = buf;
        buf = neighbor_buf;
        neighbor_buf = tmp;
    }

    page_t *page = buf->buf_page;
    page_t *neighbor_page = neighbor_buf->buf_page;

    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that this node and neighbor have swapped places
     * in the special case of this node being a leftmost child.
     */


    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */

    insertion_index = neighbor_page->num_of_keys;
    n_end = page->num_of_keys;
    if (!page->is_leaf) {
        // Append k_prime.
        neighbor_page->pairs[insertion_index].key = k_prime;
        neighbor_page->pairs[insertion_index].page_num = page->most_left_page_num;
        neighbor_page->num_of_keys++;
        
        // Pull key-pointer pairs.
        for (i = insertion_index + 1, j = 0; j < n_end; i++, j++)
            neighbor_page->pairs[i] = page->pairs[j];

        neighbor_page->num_of_keys += n_end;
        
        pagenum_t child_num;
        buf_descriptor_t *child_buf;

        // Set the parent number of the child nodes.
        for (i = insertion_index; i < neighbor_page->num_of_keys; i++)
        {
            child_num = neighbor_page->pairs[i].page_num;
            child_buf = get_buffer(table_id, child_num);
            child_buf->buf_page->parent_page_num = neighbor_buf->page_num;

            mark_buffer_dirty(child_buf);
            unpin_buffer(child_buf);
        }
    }

    /* In a leaf, append the slots and records of
     * this node to the neighbor.
     * Set the neighbor's sibling to point to
     * what had been this node's right sibling.
     */
    else {
        slot_t *left_slot;
        slot_t *right_slot;
        uint16_t val_size;
        uint16_t temp_offset = insertion_index == 0 ? PAGE_SIZE : \
            get_slot(neighbor_page->data, insertion_index - 1)->offset;

        // Pull slots and records.
        for (i = insertion_index, j = 0; j < n_end; i++, j++) {
            left_slot = get_slot(neighbor_page->data, i);
            right_slot = get_slot(page->data, j);

            val_size = right_slot->size;
            temp_offset -= val_size;

            left_slot->key = right_slot->key;
            left_slot->size = val_size;
            left_slot->offset = temp_offset;

            memcpy((char*)neighbor_page + temp_offset,
                   (char*)page + right_slot->offset, val_size);
            
            neighbor_page->amount_of_free_space -= (SLOT_SIZE + val_size);
        }

        // Set sibling page num.
        neighbor_page->num_of_keys += n_end;
        neighbor_page->right_sibling_page_num = page->right_sibling_page_num;
    }

    free_buffer(table_id, buf);
    mark_buffer_dirty(neighbor_buf);
    unpin_buffer(buf);
    unpin_buffer(neighbor_buf);

    return delete_entry(table_id, parent_buf, k_prime);
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
int redistribute_nodes(int64_t table_id, buf_descriptor_t *buf,
                       buf_descriptor_t *neighbor_buf, buf_descriptor_t *parent_buf,
                       int neighbor_index, int k_prime_index, int k_prime) {
    page_t *page = buf->buf_page;
    page_t *neighbor_page = neighbor_buf->buf_page;
    page_t *parent_page = parent_buf->buf_page;

    int i;
    buf_descriptor_t *temp_buf;
    pagenum_t temp_num;

    /* Case: this node has a neighbor to the left. 
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to this node's left end.
     */
    if (neighbor_index != -1) {
        if (!page->is_leaf) {
            // Push key-pointer pairs to the right.
            for (i = page->num_of_keys; i > 0; i--)
                page->pairs[i] = page->pairs[i - 1];

            // Set the key of the parent node.
            parent_page->pairs[k_prime_index].key =
                neighbor_page->pairs[neighbor_page->num_of_keys - 1].key;
            temp_num = neighbor_page->pairs[neighbor_page->num_of_keys - 1].page_num;

            // Pull the neighbor's last key-pointer pair.
            page->pairs[0].key = k_prime;
            page->pairs[0].page_num = page->most_left_page_num;
            page->most_left_page_num = temp_num;

            // Set the parent number of the child node.
            temp_buf = get_buffer(table_id, temp_num);
            temp_buf->buf_page->parent_page_num = buf->page_num;

            mark_buffer_dirty(temp_buf);
            unpin_buffer(temp_buf);

            (page->num_of_keys)++;
            (neighbor_page->num_of_keys)--;
        } else {
            int j, count;
            uint64_t amount_of_free_space = page->amount_of_free_space;
            slot_t *slot;
            slot_t *temp_slot;
            uint16_t val_size;
            uint16_t sum_of_val_size = 0;
            uint64_t temp_offset;

            // Measure the size of the records that should be redistributed.
            for (i = neighbor_page->num_of_keys - 1, count = 1; i >= 0; i--, count++) {
                slot = get_slot(neighbor_page->data, i);
                val_size = slot->size;
                sum_of_val_size += val_size;
                amount_of_free_space -= (SLOT_SIZE + val_size);

                if (amount_of_free_space < THRESHOLD)
                    break;
            }

            // Push records to the right.
            for (i = page->num_of_keys - 1; i >= 0; i--) {
                slot = get_slot(page->data, i);
                temp_slot = get_slot(page->data, i + count);

                val_size = slot->size;
                temp_offset = slot->offset - sum_of_val_size;

                temp_slot->key = slot->key;
                temp_slot->size = val_size;
                temp_slot->offset = temp_offset;

                memcpy((char*)page + temp_offset,
                       (char*)page + slot->offset, val_size);
            }

            // Pull neighbor's records.
            temp_offset = PAGE_SIZE;
            for(i = 0, j = neighbor_page->num_of_keys - count; i < count; i++, j++) {
                slot = get_slot(neighbor_page->data, j);
                temp_slot = get_slot(page->data, i);

                val_size = slot->size;
                temp_offset -= val_size;

                temp_slot->key = slot->key;
                temp_slot->size = val_size;
                temp_slot->offset = temp_offset;

                memcpy((char*)page + temp_offset,
                       (char*)neighbor_page + slot->offset, val_size);
                
                page->amount_of_free_space -= (SLOT_SIZE + val_size);
                neighbor_page->amount_of_free_space += (SLOT_SIZE + val_size);
            }

            page->num_of_keys += count;
            neighbor_page->num_of_keys -= count;

            parent_page->pairs[k_prime_index].key = get_slot(page->data, 0)->key;
        }
    }

    /* Case: this node is the leftmost child.
     * Take a key-pointer pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pointer pair
     * to this node's rightmost position.
     */
    else {
        if (!page->is_leaf) {
            // Set the key of the parent node.
            parent_page->pairs[k_prime_index].key = neighbor_page->pairs[0].key;
            temp_num = neighbor_page->most_left_page_num;

            // Pull the neighbor's leftmost key-pointer pair.
            page->pairs[page->num_of_keys].key = k_prime;
            page->pairs[page->num_of_keys].page_num = temp_num;
            neighbor_page->most_left_page_num = neighbor_page->pairs[0].page_num;

            // Push key-pointer pairs to the left.
            for (i = 0; i < neighbor_page->num_of_keys - 1; i++)
                neighbor_page->pairs[i] = neighbor_page->pairs[i + 1];

            // Set the parent number of the child node.
            temp_buf = get_buffer(table_id, temp_num);
            temp_buf->buf_page->parent_page_num = buf->page_num;

            mark_buffer_dirty(temp_buf);
            unpin_buffer(temp_buf);

            (page->num_of_keys)++;
            (neighbor_page->num_of_keys)--;
        } else {
            int j, count;
            uint64_t amount_of_free_space = page->amount_of_free_space;
            slot_t *slot;
            slot_t *temp_slot;
            uint16_t val_size;
            uint16_t sum_of_val_size = 0;
            uint64_t temp_offset;

            // Measure the size of the records that should be redistributed.
            for (i = 0, count = 1; i < neighbor_page->num_of_keys; i++, count++) {
                slot = get_slot(neighbor_page->data, i);
                val_size = slot->size;
                sum_of_val_size += val_size;
                amount_of_free_space -= (SLOT_SIZE + val_size);

                if (amount_of_free_space < THRESHOLD)
                    break;
            }

            // Pull neighbor's records.
            temp_offset = get_slot(page->data, page->num_of_keys - 1)->offset;
            for(i = 0, j = page->num_of_keys; i < count; i++, j++) {
                slot = get_slot(neighbor_page->data, i);
                temp_slot = get_slot(page->data, j);

                val_size = slot->size;
                temp_offset -= val_size;

                temp_slot->key = slot->key;
                temp_slot->size = val_size;
                temp_slot->offset = temp_offset;

                memcpy((char*)page + temp_offset,
                       (char*)neighbor_page + slot->offset, val_size);
                
                page->amount_of_free_space -= (SLOT_SIZE + val_size);
                neighbor_page->amount_of_free_space += (SLOT_SIZE + val_size);
            }

            // Push records to the left.
            for (i = 0; i < neighbor_page->num_of_keys - count; i++) {
                slot = get_slot(neighbor_page->data, i + count);
                temp_slot = get_slot(neighbor_page->data, i);

                val_size = slot->size;
                temp_offset = slot->offset + sum_of_val_size;

                temp_slot->key = slot->key;
                temp_slot->size = val_size;
                temp_slot->offset = temp_offset;

                memcpy((char*)neighbor_page + temp_offset,
                       (char*)neighbor_page + slot->offset, val_size);
            }

            page->num_of_keys += count;
            neighbor_page->num_of_keys -= count;

            parent_page->pairs[k_prime_index].key =
                get_slot(neighbor_page->data, 0)->key;
        }
    }

    /* This node now has one more key and one more pointer;
     * the neighbor has one fewer of each.
     */

    mark_buffer_dirty(parent_buf);
    mark_buffer_dirty(buf);
    mark_buffer_dirty(neighbor_buf);
    unpin_buffer(parent_buf);
    unpin_buffer(buf);
    unpin_buffer(neighbor_buf);

    return 0;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry(int64_t table_id, buf_descriptor_t* buf, uint64_t key) {
    
    int k_prime_index, neighbor_index;

    // Remove key and pointer from node.

    remove_entry_from_page(table_id, buf, key);

    /* Case:  deletion from the root. 
     */

    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    page_t *header_page = header_buf->buf_page;
    pagenum_t root_num = header_page->root_page_num;
    unpin_buffer(header_buf);

    if (buf->page_num == root_num) 
        return adjust_root(table_id, buf);

    page_t *page = buf->buf_page;

    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Case:  node stays at or above minimum.
     * (The simple case.)
     */
    if (!page->is_leaf) {
        if (page->num_of_keys >= cut(INTERNAL_ORDER) - 1) {
            unpin_buffer(buf);
            return 0;
        }
    } else {
        if (page->amount_of_free_space < THRESHOLD) {
            unpin_buffer(buf);
            return 0;
        }
    }

    /* Case:  node falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to this node and the pointer
     * to the neighbor.
     */

    pagenum_t neighbor_num;
    uint64_t k_prime;

    pagenum_t parent_num = page->parent_page_num;
    buf_descriptor_t *parent_buf = get_buffer(table_id, parent_num);
    page_t *parent_page = parent_buf->buf_page;

    // Find neighbor and k_prime.
    neighbor_index = get_neighbor_index(parent_page, buf->page_num);
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;

    if (neighbor_index > 0)
    {
        k_prime = parent_page->pairs[neighbor_index].key;
        neighbor_num = parent_page->pairs[neighbor_index - 1].page_num;
    } else {
        k_prime = parent_page->pairs[0].key;

        if (neighbor_index == -1)
            neighbor_num = parent_page->pairs[0].page_num;
        else
            neighbor_num = parent_page->most_left_page_num;
    }

    buf_descriptor_t *neighbor_buf = get_buffer(table_id, neighbor_num);
    page_t* neighbor_page = neighbor_buf->buf_page;
    bool is_coalescence;

    // Coalescence or Redistribution
    if (!page->is_leaf) {
        is_coalescence =
            (neighbor_page->num_of_keys + page->num_of_keys < INTERNAL_ORDER - 1);
    } else {
        is_coalescence =
            (neighbor_page->amount_of_free_space + page->amount_of_free_space >= DATA_SIZE);
    }

    /* Coalescence. */

    if (is_coalescence)
        return coalesce_nodes(table_id, buf, neighbor_buf, parent_buf, 
                              neighbor_index, k_prime);

    /* Redistribution. */

    else
        return redistribute_nodes(table_id, buf, neighbor_buf, parent_buf,
                                  neighbor_index, k_prime_index, k_prime);

    printf("never reach state, key: %lu\n", key);
    return -1;
}

int db_find_internal(int64_t table_id, int64_t key, char *ret_val,
                     uint16_t *val_size, buf_descriptor_t **leaf_buf) {
    *leaf_buf = find_leaf(table_id, key);

    if (*leaf_buf == NULL)
        return 1;

    page_t *leaf_page = (*leaf_buf)->buf_page;
    int i;
    slot_t *slot;

    // Find the key.
    for (i = 0; i < leaf_page->num_of_keys; i++) {
        slot = get_slot(leaf_page->data, i);

        if (slot->key == key)
            break;
    }

    // The key exists.
    if (i < leaf_page->num_of_keys) {
        if (ret_val != NULL) {
            memcpy(ret_val, (char*)leaf_page + slot->offset, slot->size);
            *val_size = slot->size;
        }

        return 0;
    }

    return 2;
}

// Open an existing database file or create one if not exist.
int64_t open_table(const char *pathname) {
    return buffer_open_table(pathname);
}

// Insert a record to the given table.
int db_insert(int64_t table_id, int64_t key, const char *value, uint16_t val_size) {
    if (val_size < MIN_VALUE_SIZE || val_size > MAX_VALUE_SIZE)
        return 1;
    
    buf_descriptor_t *leaf_buf;
    int ret = db_find_internal(table_id, key, NULL, NULL, &leaf_buf);

    // The key already exists.
    if (ret == 0) {
        unpin_buffer(leaf_buf);
        return 1;
    }

    // The first insertion
    if (ret == 1)
        return start_new_tree(table_id, key, value, val_size);

    // Insert the record directly into the leaf page.
    if (leaf_buf->buf_page->amount_of_free_space >= (SLOT_SIZE + val_size))
        return insert_into_leaf(table_id, leaf_buf, key, value, val_size);

    // Insert the record with splitting.
    return insert_into_leaf_after_splitting(table_id, leaf_buf, key, value, val_size);
}

// Find a record with the matching key from the given table.
int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size) {
    buf_descriptor_t *leaf_buf;
    int ret = db_find_internal(table_id, key, ret_val, val_size, &leaf_buf);

    if (leaf_buf)
        unpin_buffer(leaf_buf);

    return ret;
}

// Delete a record with the matching key from the given table.
int db_delete(int64_t table_id, int64_t key) {
    buf_descriptor_t *leaf_buf;
    int ret = db_find_internal(table_id, key, NULL, NULL, &leaf_buf);

    // The key does not exist.
    if (ret != 0)
        return 1;

    return delete_entry(table_id, leaf_buf, key);
}

// Find records with a key betwen the range: begin_key <= key <= end_key
int db_scan(int64_t table_id, int64_t begin_key, int64_t end_key,
            std::vector<int64_t> *keys, std::vector<char*> *values,
            std::vector<uint16_t> *val_sizes) {

    buf_descriptor_t *leaf_buf = find_leaf(table_id, begin_key);

    // There is no root page.
    if (leaf_buf == NULL)
        return 1;

    page_t *leaf_page = leaf_buf->buf_page;
    int i = 0;
    slot_t *slot;
    db_key_t temp_key = begin_key;
    char *temp_value;
    uint16_t val_size;
    pagenum_t sibling_num;
    bool reached_end = false;

    // Find the leaf page that contains begin_key.
    while (temp_key <= end_key) {
        slot = get_slot(leaf_page->data, i);
        temp_key = slot->key;

        if (temp_key >= begin_key)
            break;

        if (i < leaf_page->num_of_keys - 1) {
            i++;
        } else {
            sibling_num = leaf_page->right_sibling_page_num;

            if (sibling_num == -1) {
                reached_end = true;
                break;
            }

            unpin_buffer(leaf_buf);

            leaf_buf = get_buffer(table_id, sibling_num);
            leaf_page = leaf_buf->buf_page;
            i = 0;
        }
    }

    // There is no key for this range.
    if (temp_key > end_key || reached_end) {
        unpin_buffer(leaf_buf);
        return 1;
    }

    // Scan leaf pages.
    while (temp_key <= end_key) {
        val_size = slot->size;
        keys->push_back(temp_key);
        temp_value = (char*)calloc(1, val_size);

        memcpy(temp_value, (char*)leaf_page + slot->offset, val_size);
        values->push_back(temp_value);
        val_sizes->push_back(val_size);
        
        if (i < leaf_page->num_of_keys - 1) {
            i++;
        } else {
            sibling_num = leaf_page->right_sibling_page_num;

            if (sibling_num == -1)
                break;

            unpin_buffer(leaf_buf);

            leaf_buf = get_buffer(table_id, sibling_num);
            leaf_page = leaf_buf->buf_page;
            i = 0;
        }

        slot = get_slot(leaf_page->data, i);
        temp_key = slot->key;
    }

    unpin_buffer(leaf_buf);
    return 0;
}

// Initialize the database system.
int init_db(uint32_t num_ht_entries, uint32_t num_buf) {
    return init_buffer_pool(num_ht_entries, num_buf);
}

// Shutdown the databasee system.
int shutdown_db() {
    return close_buffer_pool();
}