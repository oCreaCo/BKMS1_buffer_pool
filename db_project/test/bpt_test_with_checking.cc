#include "db.h"

#include <gtest/gtest.h>

#include <pthread.h>
#include <string>

class InfoCout : public std::stringstream
{
public:
    ~InfoCout()
    {
        std::cerr << "\u001b[32m[   INFO   ] \u001b[0m" << str() << std::flush;
    }
};

class StatCout : public std::stringstream
{
public:
    ~StatCout()
    {
        std::cerr << "\u001b[32m[   STAT   ] \u001b[33m" << str() << "\u001b[0m" << std::flush;
    }
};

#define INFO_COUT InfoCout()
#define STAT_COUT StatCout()

#define TIMEOUT (120) // sec
#define SUCCESS (0)

#define GetElapsedTime(begin, end) \
    ((end.tv_sec * 1000000000 + end.tv_nsec) - \
     (begin.tv_sec * 1000000000 + begin.tv_nsec))

#define CheckTimeout(test) \
do { \
    INFO_COUT << #test << " started (timeout: " << TIMEOUT << " s)" << std::endl; \
    \
    ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &begin), 0); /* error handling */ \
    \
    ASSERT_EQ(clock_gettime(CLOCK_REALTIME, &timeout), 0); /* error handling */ \
    timeout.tv_sec += TIMEOUT; /* set timeout */ \
    \
    ret = NULL; \
    pthread_create(&task_thread, NULL, test, this); /* do task concurrently */ \
    EXPECT_EQ(pthread_timedjoin_np(task_thread, (void**)(&ret), &timeout), 0); /* wait for the task thread with timeout */ \
    \
    ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &end), 0); /* error handling */ \
    \
    if (ret) { \
        EXPECT_EQ(*ret, SUCCESS); \
        free(ret); \
    } \
    \
    INFO_COUT << #test << " finished (elapsed: " << string_format("%.3f", (float)(GetElapsedTime(begin, end)) / 1000000000) << " s)" << std::endl; \
} while(0)

int64_t table_id;      // file descriptor
std::string pathname;  // path for the file
uint32_t num_ht_entries;
uint32_t num_buf;
uint32_t num_keys;
uint32_t num_deletion;
char buf[MAX_VALUE_SIZE + 1];

int64_t hit_ratios[10];

struct timespec begin, end, timeout;

/*
 * TestFixture for B+ tree operation tests
 */
class BptTestWithChecking : public ::testing::Test {
    protected:

    BptTestWithChecking() {}

    void InitTest(uint32_t _num_ht_entries, uint32_t _num_buf,
			      uint32_t _num_keys, uint32_t _num_deletion) {
        num_ht_entries = _num_ht_entries;
        num_buf = _num_buf;
        num_keys = _num_keys;
        num_deletion = _num_deletion;
        pathname = "bpt_test_hit_ratio.db";
    }

    void InitTable() {
        EXPECT_EQ(init_db(num_ht_entries, num_buf), 0);
        table_id = open_table(pathname.c_str());
        EXPECT_TRUE(table_id >= 0);
    }

    void *InsertTestInternal(void) {
        uint16_t val_size;
        int x, y, tmp;
        int num_inserted = 0;
        int mixingCount = num_keys * 3 / 2;
        const char *charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        int *ret = (int*)malloc(sizeof(int));

        InitTable();

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

        if (num_inserted != num_keys) {
            *ret = 1;
            return (void*)ret;
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        free(keys);

        hit_ratios[0] = get_buffer_hit_ratio();

        shutdown_db();

        *ret = SUCCESS;
        return (void*)ret;
    }

    static void *InsertTest(void *context) {
        return ((BptTestWithChecking *)context)->InsertTestInternal();
    }

    void *FindTest1Internal(void) {
        uint16_t val_size;
        int num_found = 0;
        int *ret = (int*)malloc(sizeof(int));

        InitTable();

        for (int i = 0; i < num_keys; i++) {
            memset(buf, 0, MAX_VALUE_SIZE + 1);
            if (db_find(table_id, i, buf, &val_size) == 0) {
                if (val_size < MIN_VALUE_SIZE) {
                    *ret = 1;
                    return (void*)ret;
                }

                if (val_size > MAX_VALUE_SIZE) {
                    *ret = 2;
                    return (void*)ret;
                }

                num_found++;
            }
        }

        if (num_found != num_keys) {
            *ret = 3;
            return (void*)ret;
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        hit_ratios[0] = get_buffer_hit_ratio();

        shutdown_db();

        *ret = SUCCESS;
        return (void*)ret;
    }

    static void *FindTest1(void *context) {
        return ((BptTestWithChecking *)context)->FindTest1Internal();
    }

    void *DeleteTestInternal(void) {
        int x, y, tmp;
        int num_deleted = 0;
        int mixingCount = num_keys * 3 / 2;
        int *ret = (int*)malloc(sizeof(int));

        InitTable();

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

        if (num_deleted != num_deletion) {
            *ret = 1;
            return (void*)ret;
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        free(keys);

        hit_ratios[0] = get_buffer_hit_ratio();

        shutdown_db();

        *ret = SUCCESS;
        return ret;
    }

    static void *DeleteTest(void *context) {
        return ((BptTestWithChecking *)context)->DeleteTestInternal();
    }

    void *FindTest2Internal(void) {
        uint16_t val_size;
        int num_found = 0;
        int *ret = (int*)malloc(sizeof(int));

        InitTable();

        for (int i = 0; i < num_keys; i++) {
            memset(buf, 0, MAX_VALUE_SIZE + 1);
            if (db_find(table_id, i, buf, &val_size) == 0) {
                if (val_size < MIN_VALUE_SIZE) {
                    *ret = 1;
                    return (void*)ret;
                }

                if (val_size > MAX_VALUE_SIZE) {
                    *ret = 2;
                    return (void*)ret;
                }

                num_found++;
            }
        }

        if (num_found != (num_keys - num_deletion)) {
            *ret = 3;
            return (void*)ret;
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        hit_ratios[0] = get_buffer_hit_ratio();

        shutdown_db();

        *ret = SUCCESS;
        return (void*)ret;
    }

    static void *FindTest2(void *context) {
        return ((BptTestWithChecking *)context)->FindTest2Internal();
    }

    void *ScanTestInternal(void) {
        int num_found = 0;
        int *ret = (int*)malloc(sizeof(int));

        InitTable();

        std::vector<int64_t> *s_keys = new std::vector<int64_t>();
        std::vector<char*> *s_values = new std::vector<char*>();
        std::vector<uint16_t> *s_val_sizes = new std::vector<uint16_t>();

        if (db_scan(table_id, -1, num_keys + 1, s_keys, s_values, s_val_sizes) != 0) {
            *ret = 1;
            return (void*)ret;
        }

        if (s_keys->size() != (num_keys - num_deletion)) {
            *ret = 2;
            return (void*)ret;
        }

        for (int i = 0; i < s_values->size(); i++) {
            free((*s_values)[i]);

            if ((*s_val_sizes)[i] < MIN_VALUE_SIZE) {
                *ret = 3;
                return (void*)ret;
            }

            if ((*s_val_sizes)[i] > MAX_VALUE_SIZE) {
                *ret = 4;
                return (void*)ret;
            }
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        hit_ratios[0] = get_buffer_hit_ratio();

        shutdown_db();

        remove(pathname.c_str());

        *ret = SUCCESS;
        return (void*)ret;
    }

    static void *ScanTest(void *context) {
        return ((BptTestWithChecking *)context)->ScanTestInternal();
    }

    void *OverallOpsTestInternal(void) {
        uint16_t val_size;
        int x, y, tmp;
        int num_inserted = 0;
        int num_deleted = 0;
        int num_found;
        int mixingCount = num_keys * 3 / 2;
        const char *charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        int *ret = (int*)malloc(sizeof(int));

        InitTable();

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

        if (num_inserted != num_keys) {
            *ret = 1;
            return (void*)ret;
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        hit_ratios[0] = get_buffer_hit_ratio();

        num_found = 0;
        for (int i = 0; i < num_keys; i++) {
            memset(buf, 0, MAX_VALUE_SIZE + 1);
            if (db_find(table_id, i, buf, &val_size) == 0) {
                if (val_size < MIN_VALUE_SIZE) {
                    *ret = 2;
                    return (void*)ret;
                }

                if (val_size > MAX_VALUE_SIZE) {
                    *ret = 3;
                    return (void*)ret;
                }
                num_found++;
            }
        }

        if (num_found != num_keys) {
            *ret = 4;
            return (void*)ret;
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        hit_ratios[1] = get_buffer_hit_ratio();

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

        if (num_deleted != num_deletion) {
            *ret = 5;
            return (void*)ret;
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        hit_ratios[2] = get_buffer_hit_ratio();

        num_found = 0;
        for (int i = 0; i < num_keys; i++) {
            memset(buf, 0, MAX_VALUE_SIZE + 1);
            if (db_find(table_id, i, buf, &val_size) == 0) {
                if (val_size < MIN_VALUE_SIZE) {
                    *ret = 6;
                    return (void*)ret;
                }

                if (val_size > MAX_VALUE_SIZE) {
                    *ret = 7;
                    return (void*)ret;
                }

                num_found++;
            }
        }

        if (num_found != (num_keys - num_deletion)) {
            *ret = 8;
            return (void*)ret;
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        hit_ratios[3] = get_buffer_hit_ratio();

        std::vector<int64_t> *s_keys = new std::vector<int64_t>();
        std::vector<char*> *s_values = new std::vector<char*>();
        std::vector<uint16_t> *s_val_sizes = new std::vector<uint16_t>();
        num_found = 0;

        if (db_scan(table_id, -1, num_keys + 1, s_keys, s_values, s_val_sizes) != 0) {
            *ret = 9;
            return (void*)ret;
        }

        if (s_keys->size() != (num_keys - num_deletion)) {
            *ret = 10;
            return (void*)ret;
        }

        for (int i = 0; i < s_values->size(); i++) {
            free((*s_values)[i]);

            if ((*s_val_sizes)[i] < MIN_VALUE_SIZE) {
                *ret = 11;
                return (void*)ret;
            }

            if ((*s_val_sizes)[i] > MAX_VALUE_SIZE) {
                *ret = 12;
                return (void*)ret;
            }
        }

        STAT_COUT << get_buffer_stat() << std::endl;

        hit_ratios[4] = get_buffer_hit_ratio();

        shutdown_db();

        remove(pathname.c_str());

        *ret = SUCCESS;
        return (void*)ret;
    }

    static void *OverallOpsTest(void *context) {
        return ((BptTestWithChecking *)context)->OverallOpsTestInternal();
    }

};

TEST_F(BptTestWithChecking, LargeHashtableLargeBuffer) {
    int *ret;
    int64_t hit_ratio_baselines[5] = { 95, 95, 95, 95, 95 };
    pthread_t task_thread;

    InitTest(5000, 256, 5000, 100);

    CheckTimeout(InsertTest);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(FindTest1);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(DeleteTest);
    EXPECT_GE(hit_ratios[0], 75);

    CheckTimeout(FindTest2);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(ScanTest);
    EXPECT_EQ(hit_ratios[0], 0);

    CheckTimeout(OverallOpsTest);
    for (int i = 0; i < 5; i++)
        EXPECT_GE(hit_ratios[i], hit_ratio_baselines[i]);
}

TEST_F(BptTestWithChecking, SmallHashtableLargeBuffer) {
    int *ret;
    int64_t hit_ratio_baselines[5] = { 95, 95, 95, 95, 95 };
    pthread_t task_thread;

    InitTest(10, 256, 5000, 100);

    CheckTimeout(InsertTest);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(FindTest1);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(DeleteTest);
    EXPECT_GE(hit_ratios[0], 75);

    CheckTimeout(FindTest2);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(ScanTest);
    EXPECT_EQ(hit_ratios[0], 0);

    CheckTimeout(OverallOpsTest);
    for (int i = 0; i < 5; i++)
        EXPECT_GE(hit_ratios[i], hit_ratio_baselines[i]);
}

TEST_F(BptTestWithChecking, LargeHashtableSmallBuffer) {
    int *ret;
    int64_t hit_ratio_baselines[5] = { 75, 85, 85, 85, 85 };
    pthread_t task_thread;

    InitTest(5000, 32, 5000, 100);

    CheckTimeout(InsertTest);
    EXPECT_GE(hit_ratios[0], 75);

    CheckTimeout(FindTest1);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(DeleteTest);
    EXPECT_GE(hit_ratios[0], 70);

    CheckTimeout(FindTest2);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(ScanTest);
    EXPECT_EQ(hit_ratios[0], 0);

    CheckTimeout(OverallOpsTest);
    for (int i = 0; i < 5; i++)
        EXPECT_GE(hit_ratios[i], hit_ratio_baselines[i]);
}

TEST_F(BptTestWithChecking, SmallHashtableSmallBuffer) {
    int *ret;
    int64_t hit_ratio_baselines[5] = { 75, 85, 85, 85, 85 };
    pthread_t task_thread;

    InitTest(100, 32, 5000, 100);

    CheckTimeout(InsertTest);
    EXPECT_GE(hit_ratios[0], 75);

    CheckTimeout(FindTest1);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(DeleteTest);
    EXPECT_GE(hit_ratios[0], 70);

    CheckTimeout(FindTest2);
    EXPECT_GE(hit_ratios[0], 95);

    CheckTimeout(ScanTest);
    EXPECT_EQ(hit_ratios[0], 0);

    CheckTimeout(OverallOpsTest);
    for (int i = 0; i < 5; i++)
        EXPECT_GE(hit_ratios[i], hit_ratio_baselines[i]);
}
