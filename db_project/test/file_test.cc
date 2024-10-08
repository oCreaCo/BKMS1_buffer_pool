#include "file.h"

#include <gtest/gtest.h>

#include <string>

/*******************************************************************************
 * The test structures stated here were written to give you and idea of what a
 * test should contain and look like. Feel free to change the code and add new
 * tests of your own. The more concrete your tests are, the easier it'd be to
 * detect bugs in the future projects.
 ******************************************************************************/

/*
 * Tests file open/close APIs.
 * 1. Open a file and check the descriptor
 * 2. Check if the file's initial size is 10 MiB
 */
TEST(FileInitTest, HandlesInitialization) {
    int64_t table_id;                       // table id
    std::string pathname = "init_test.db";  // customize it to your test file

    // Open a table file
    table_id = file_open_table_file(pathname.c_str());

    // Check if the file is opened
    ASSERT_TRUE(table_id >= 0);  // change the condition to your design's behavior

    // Check the size of the initial file
    page_t *header_page = (page_t*)malloc(PAGE_SIZE);
    file_read_page(table_id, 0, header_page);

    int num_pages = /* fetch the number of pages from the header page */ header_page->num_of_pages;
    EXPECT_EQ(num_pages, INITIAL_DB_FILE_SIZE / PAGE_SIZE)
              << "The initial number of pages does not match the requirement: "
              << num_pages;

    // Close all table files
    file_close_table_files();

    // Remove the db file
    int is_removed = remove(pathname.c_str());

    ASSERT_EQ(is_removed, /* 0 for success */ 0);
}

TEST(FileInitTest, CheckDoubling) {
    int64_t table_id;                           // table id
    std::string pathname = "doubling_test.db";  // customize it to your test file

    // Open a table file
    table_id = file_open_table_file(pathname.c_str());

    // Check if the file is opened
    ASSERT_TRUE(table_id >= 0);  // change the condition to your design's behavior

    // Check the size of the initial file
    page_t *header_page = (page_t*)malloc(PAGE_SIZE);
    file_read_page(table_id, 0, header_page);

    // fetch the number of pages from the header page
    int num_pages = header_page->num_of_pages;
    
    // Alloc pages more than init-free pages
    for (int i = 0; i < num_pages; ++i)
        file_alloc_page(table_id);

    // Init to check doubled free pages
    file_read_page(table_id, 0, header_page);
    pagenum_t free_page_num = header_page->free_page_num;
    page_t *tmp = (page_t*)malloc(PAGE_SIZE);
    int num_of_free_pages = 0;

    // Count free pages with while-loop
    while(free_page_num != -1) {
        ++num_of_free_pages;

        file_read_page(table_id, free_page_num, tmp);
        free_page_num = tmp->next_free_page_num;
    }

    EXPECT_EQ(num_of_free_pages, num_pages - 1);
    EXPECT_EQ(num_pages * 2, header_page->num_of_pages);

    // Close all table files
    file_close_table_files();

    // Remove the db file
    int is_removed = remove(pathname.c_str());

    free(header_page);
    free(tmp);

    ASSERT_EQ(is_removed, /* 0 for success */ 0);
}

/*
 * TestFixture for page allocation/deallocation tests
 */
class FileTest : public ::testing::Test {
    protected:
    /*
     * NOTE: You can also use constructor/destructor instead of SetUp() and
     * TearDown(). The official document says that the former is actually
     * perferred due to some reasons. Checkout the document for the difference
     */
    FileTest() { 
        pathname = "file_test.db";
        table_id = file_open_table_file(pathname.c_str());
    }

    ~FileTest() {
        if (table_id >= 0) {
            file_close_table_files();
            remove(pathname.c_str());
        }
    }

    int64_t table_id;      // file descriptor
    std::string pathname;  // path for the file
};

/*
 * Tests page allocation and free
 * - Allocate 2 pages and free one of them, traverse the free page list
 *    and check the existence/absence of the freed/allocated page
 */
TEST_F(FileTest, HandlesPageAllocation) {
    pagenum_t allocated_page, freed_page;

    // Allocate the pages
    allocated_page = file_alloc_page(table_id);
    freed_page = file_alloc_page(table_id);

    // Free one page
    file_free_page(table_id, freed_page);

    // Traverse the free page list and check the existence of the freed/allocated
    // pages. You might need to open a few APIs soley for testing.
    bool is_freed_in = false;
    bool is_allocated_in = false;
    
    // Read header page and free page num
    page_t *header_page = (page_t*)malloc(PAGE_SIZE);
    file_read_page(table_id, 0, header_page);
    pagenum_t free_page_num = header_page->free_page_num;

    page_t *tmp = (page_t*)malloc(PAGE_SIZE);
    int num_of_free_pages = 0;

    // Check if freed page is in list, if allocated page isn't in list,
    // count all free pages
    while(free_page_num != -1) {
        if (free_page_num == freed_page) is_freed_in = true;
        if (free_page_num == allocated_page) is_allocated_in = true;
        ++num_of_free_pages;

        file_read_page(table_id, free_page_num, tmp);
        free_page_num = tmp->next_free_page_num;
    }

    EXPECT_EQ(num_of_free_pages, header_page->free_page_num);

    file_free_page(table_id, allocated_page);
    free(tmp);

    ASSERT_TRUE(is_freed_in);
    ASSERT_FALSE(is_allocated_in);
}

/*
 * Tests page read/write operations
 * - Write/Read a page with some random content and check if the data matches
 */
TEST_F(FileTest, CheckReadWriteOperation) {
    char str_part[17] = "0123456789ABCDEF";
    char src[PAGE_SIZE];
    char dest[PAGE_SIZE];

    // Fill src
    int quotient = PAGE_SIZE / 16;
    int remainder = PAGE_SIZE % 16;
    int i = 0, pos = 0;

    for(; i < quotient; ++i, pos += 16) {
        memcpy((void*)(src + pos), (void*)str_part, 16);
    }

    if (remainder != 0) {
        memcpy((void*)(src + pos), (void*)str_part, remainder);
    }

    // Alloc new page and do I/O
    bool is_diff = false;
    pagenum_t tmp = file_alloc_page(table_id);

    file_write_page(table_id, tmp, (page_t*)src);
    file_read_page(table_id, tmp, (page_t*)dest);

    // Check all characters
    for (i = 0; i < PAGE_SIZE; ++i) {
        if (src[i] != dest[i]) {
            is_diff = true;
            break;
        }
    }

    ASSERT_FALSE(is_diff);
}
