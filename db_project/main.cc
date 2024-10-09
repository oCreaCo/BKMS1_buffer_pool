#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "db.h"

// MAIN
int main( int argc, char ** argv ) {
    int64_t table_id;
    db_key_t key;
    db_key_t key2;
    uint16_t value_size;
    char buffer[MAX_VALUE_SIZE + 1];
    int ret;
    char instruction;
    
    std::vector<int64_t> *s_keys = new std::vector<int64_t>();
    std::vector<char*> *s_values = new std::vector<char*>();
    std::vector<uint16_t> *s_val_sizes = new std::vector<uint16_t>();

    if (init_db(8, 4))
        return 1;

    printf("input db name\n");
    scanf("%s", buffer);
    getchar();
    table_id = open_table(buffer);

    while (true) {
        printf("(i/f/d/s/q/p) > ");
        scanf("%c", &instruction);

        switch (instruction) {
        case 'd':
            printf("input delete key\n");
            scanf("%ld", &key);
            getchar();

            ret = db_delete(table_id, key);

            if (!ret)
                printf("Deletion successed\n");
            else
                printf("Deleteion failed\n");

            break;
        case 'f':
            printf("input find key\n");
            scanf("%ld", &key);
            getchar();

            memset(buffer, 0, MAX_VALUE_SIZE + 1);
            ret = db_find(table_id, key, buffer, &value_size);
            buffer[value_size] = '\0';

            if (!ret)
                printf("value: %s, size: %hu\n", buffer, value_size);
            else
                printf("Find failed\n");

            break;
        case 'i':
            printf("input insert key\n");
            scanf("%ld", &key);
            getchar();

            printf("input value string (min length: %d, max length: %d)\n", MIN_VALUE_SIZE, MAX_VALUE_SIZE);
            scanf("%s", buffer);
            getchar();

            ret = db_insert(table_id, key, buffer, strlen(buffer));

            if (!ret)
                printf("Insertion successed\n");
            else
                printf("Insertion failed\n");

            break;
        case 's':
            printf("input scan begin key, end key\n");
            scanf("%ld %ld", &key, &key2);
            getchar();

            ret = db_scan(table_id, key, key2, s_keys, s_values, s_val_sizes);

            if (!ret) {
                for (int k = 0; k < s_keys->size(); ++k) {
                    printf("scanned key: %ld, value: %s, size: %hu\n", (*s_keys)[k], (*s_values)[k], (*s_val_sizes)[k]);
                    free((*s_values)[k]);
                }

                s_keys->clear();
                s_values->clear();
                s_val_sizes->clear();
            }
            else
                printf("Scan failed\n");

            break;
        case 'p':
            print_buffer_stat();
            break;
        case 'q':
            printf("Exit\n");
            while (getchar() != (int)'\n');
            shutdown_db();
            return EXIT_SUCCESS;
        default:
            printf("\n");
            break;
        }
    }

    return EXIT_FAILURE;
}
