#include "db.h"

#include <gtest/gtest.h>

#include <string>

/*******************************************************************************
 * The test structures stated here were written to give you and idea of what a
 * test should contain and look like. Feel free to change the code and add new
 * tests of your own. The more concrete your tests are, the easier it'd be to
 * detect bugs in the future projects.
 ******************************************************************************/

/*
 * TestFixture for B+ tree operation tests
 */
class BptTest : public ::testing::Test {
    protected:
    /*
     * NOTE: You can also use constructor/destructor instead of SetUp() and
     * TearDown(). The official document says that the former is actually
     * perferred due to some reasons. Checkout the document for the difference
     */
    BptTest() { 
        pathname = "bpt_test.db";
        init_db(num_keys, 512);
        table_id = open_table(pathname.c_str());
    }

    ~BptTest() {
        if (table_id >= 0)
            shutdown_db();
    }

    int64_t table_id;      // file descriptor
    std::string pathname;  // path for the file
    int32_t num_keys = 5000;
	int32_t num_deletion = 100;
    char buf[MAX_VALUE_SIZE + 1];
};

/*
 * Tests record insertion
 * - Insert keys in random order
 */
TEST_F(BptTest, InsertTest) {
    uint16_t val_size;
    int x, y, tmp;
    int num_inserted = 0;
    int mixingCount = num_keys * 3 / 2;
    const char *charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    ASSERT_TRUE(table_id >= 0);

    //get random keys
    db_key_t *keys = (db_key_t*)malloc(num_keys * sizeof(db_key_t));
    srand(time(NULL));

    for (int i = 0; i < num_keys; i++)
        keys[i] = i;

    for (int i = 0; i < mixingCount; i++) {
        x = rand() % num_keys;
        y = rand() % num_keys;

        if (x != y)
        {
            tmp = keys[x];
            keys[x] = keys[y];
            keys[y] = tmp;
        }
    }

    for (int i = 0; i < num_keys; i++) {
        sprintf(buf, "%-8ld", keys[i]);

        val_size = (MIN_VALUE_SIZE + rand() % (MAX_VALUE_SIZE - MIN_VALUE_SIZE + 1));
        for (int j = 8; j < val_size; j++)
            buf[j] = charset[rand() % 62];

        buf[val_size] = '\0';
        if (db_insert(table_id, keys[i], buf, val_size) == 0)
            num_inserted++;
    }

    ASSERT_EQ(num_inserted, num_keys);

    free(keys);
}

/*
 * Tests record search
 * - Search records using inserted keys
 */
TEST_F(BptTest, FindTest1) {
    uint16_t val_size;
    int num_found = 0;

    ASSERT_TRUE(table_id >= 0);

    for (int i = 0; i < num_keys; i++) {
        memset(buf, 0, MAX_VALUE_SIZE + 1);
        if (db_find(table_id, i, buf, &val_size) == 0) {
            ASSERT_GE(val_size, MIN_VALUE_SIZE);
            ASSERT_LE(val_size, MAX_VALUE_SIZE);
            num_found++;
        }
    }

    ASSERT_EQ(num_found, num_keys);
}

/*
 * Tests record deletion
 * - Delete some records in random order
 */
TEST_F(BptTest, DeleteTest) {
    int x, y, tmp;
    int num_deleted = 0;
    int mixingCount = num_keys * 3 / 2;

    ASSERT_TRUE(table_id >= 0);

    db_key_t *keys = (db_key_t*)malloc(num_keys * sizeof(db_key_t));
    srand(time(NULL));

    for (int i = 0; i < num_keys; i++)
        keys[i] = i;

    for (int i = 0; i < mixingCount; i++) {
        x = rand() % num_keys;
        y = rand() % num_keys;

        if (x != y)
        {
            tmp = keys[x];
            keys[x] = keys[y];
            keys[y] = tmp;
        }
    }

    for (int i = 0; i < num_deletion; i++) {
        if (db_delete(table_id, keys[i]) == 0)
            num_deleted++;
    }

    ASSERT_EQ(num_deleted, num_deletion);

    free(keys);
}

/*
 * Tests record search
 * - Search records using inserted keys and delete keys
 */
TEST_F(BptTest, FindTest2) {
    uint16_t val_size;
    int num_found = 0;

    ASSERT_TRUE(table_id >= 0);

    for (int i = 0; i < num_keys; i++) {
        memset(buf, 0, MAX_VALUE_SIZE + 1);
        if (db_find(table_id, i, buf, &val_size) == 0) {
            ASSERT_GE(val_size, MIN_VALUE_SIZE);
            ASSERT_LE(val_size, MAX_VALUE_SIZE);
            num_found++;
        }
    }

    ASSERT_EQ(num_found, num_keys - num_deletion);
}

/*
 * Tests record scan
 * - Scan remaining records
 */
TEST_F(BptTest, ScanTest) {
    int num_found = 0;

    ASSERT_TRUE(table_id >= 0);

    std::vector<int64_t> *s_keys = new std::vector<int64_t>();
    std::vector<char*> *s_values = new std::vector<char*>();
    std::vector<uint16_t> *s_val_sizes = new std::vector<uint16_t>();

    ASSERT_EQ(db_scan(table_id, -1, num_keys + 1, s_keys, s_values, s_val_sizes), 0);
    ASSERT_EQ(s_keys->size(), num_keys - num_deletion);

    for (int i = 0; i < s_values->size(); i++) {
        ASSERT_GE((*s_val_sizes)[i], MIN_VALUE_SIZE);
        ASSERT_LE((*s_val_sizes)[i], MAX_VALUE_SIZE);
        free((*s_values)[i]);
    }

    remove(pathname.c_str());
}
/*
 * Tests overall operations
 * - Execute the above tests consecutively without closing the DB
 */
TEST_F(BptTest, OverallOpsTest) {
    uint16_t val_size;
    int x, y, tmp;
    int num_inserted = 0;
    int num_deleted = 0;
    int num_found;
    int mixingCount = num_keys * 3 / 2;
    const char *charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    ASSERT_TRUE(table_id >= 0);

    //get random keys
    db_key_t *keys = (db_key_t*)malloc(num_keys * sizeof(db_key_t));
    srand(time(NULL));

    for (int i = 0; i < num_keys; i++)
        keys[i] = i;

    for (int i = 0; i < mixingCount; i++) {
        x = rand() % num_keys;
        y = rand() % num_keys;

        if (x != y)
        {
            tmp = keys[x];
            keys[x] = keys[y];
            keys[y] = tmp;
        }
    }

    for (int i = 0; i < num_keys; i++) {
        sprintf(buf, "%-8ld", keys[i]);

        val_size = (MIN_VALUE_SIZE + rand() % (MAX_VALUE_SIZE - MIN_VALUE_SIZE + 1));
        for (int j = 8; j < val_size; j++)
            buf[j] = charset[rand() % 62];

        buf[val_size] = '\0';
        if (db_insert(table_id, keys[i], buf, val_size) == 0)
            num_inserted++;
    }

    ASSERT_EQ(num_inserted, num_keys);

    for (int i = 0; i < num_keys; i++) {
        memset(buf, 0, MAX_VALUE_SIZE + 1);
        if (db_find(table_id, i, buf, &val_size) == 0) {
            ASSERT_GE(val_size, MIN_VALUE_SIZE);
            ASSERT_LE(val_size, MAX_VALUE_SIZE);
            num_found++;
        }
    }

    ASSERT_EQ(num_found, num_keys);

    for (int i = 0; i < mixingCount; i++) {
        x = rand() % num_keys;
        y = rand() % num_keys;

        if (x != y)
        {
            tmp = keys[x];
            keys[x] = keys[y];
            keys[y] = tmp;
        }
    }

    for (int i = 0; i < num_deletion; i++) {
        if (db_delete(table_id, keys[i]) == 0)
            num_deleted++;
    }

    ASSERT_EQ(num_deleted, num_deletion);

    num_found = 0;
    for (int i = 0; i < num_keys; i++) {
        memset(buf, 0, MAX_VALUE_SIZE + 1);
        if (db_find(table_id, i, buf, &val_size) == 0) {
            ASSERT_GE(val_size, MIN_VALUE_SIZE);
            ASSERT_LE(val_size, MAX_VALUE_SIZE);
            num_found++;
        }
    }

    ASSERT_EQ(num_found, num_keys - num_deletion);

    std::vector<int64_t> *s_keys = new std::vector<int64_t>();
    std::vector<char*> *s_values = new std::vector<char*>();
    std::vector<uint16_t> *s_val_sizes = new std::vector<uint16_t>();
    num_found = 0;

    ASSERT_EQ(db_scan(table_id, -1, num_keys + 1, s_keys, s_values, s_val_sizes), 0);
    ASSERT_EQ(s_keys->size(), num_keys - num_deletion);

    for (int i = 0; i < s_values->size(); i++) {
        ASSERT_GE((*s_val_sizes)[i], MIN_VALUE_SIZE);
        ASSERT_LE((*s_val_sizes)[i], MAX_VALUE_SIZE);
        free((*s_values)[i]);
    }

    remove(pathname.c_str());
}
