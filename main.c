#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char device[50];        
    char date[8];          
    float temperature;      
} Record;

int check_date(char *date) {
    int year, month;
    if (sscanf(date, "%d-%d", &year, &month) != 2) {
        return 0;
    }
    return year > 2024 || year == 2024 && month >= 3;
}

int main() {
    FILE* file = fopen("devices.csv", "r");
    int total_count = 0;
    int capacity = 1000;
    Record *records = malloc(capacity * sizeof(Record));

    char buffer[1024];
    fgets(buffer, sizeof(buffer), file);

    while (fgets(buffer, sizeof(buffer), file)) {
        if (total_count >= capacity) {
            capacity *= 2;
            records = realloc(records, capacity * sizeof(Record));
        }

        Record r;
        char *tk;

        tk = strtok(buffer, "|");
        if (!tk) continue;

        tk = strtok(NULL, "|");
        if (!tk) continue;
        strncpy(r.device, tk, sizeof(r.device));
        r.device[sizeof(r.device) - 1] = '\0';

        tk = strtok(NULL, "|");
        if (!tk) continue;

        tk = strtok(NULL, "|");
        if (!tk || !check_date(tk)) continue;
        strncpy(r.date, tk, 7); 
        r.date[8] = '\0';

        tk = strtok(NULL, "|");
        if (!tk) continue;
        r.temperature = atof(tk);

        records[total_count++] = r;
    }

    // for (int i = 0; i <= 50000; i++) {
    //     printf("Device: %s | Data: %s | Temp: %.2f\n",
    //     records[i].device, records[i].date, records[i].temperature);
    // }

    char cur_date[8] = "2024-03";
    char date[8];
    strncpy(date, records[0].date, 7);
    date[7] = '\0';
    Record *record_by_date[100];
    int chunk_index = 0;

    for (int i = 1; i < total_count; i++) {
        strncpy(date, records[i].date, 7);
        date[7] = '\0';
        // printf("date: %s", date);
        if (strcmp(cur_date, date) != 0) {
            strncpy(cur_date, date, 8);
            record_by_date[chunk_index] = &records[i];
            chunk_index++;
        }
    }
 
    printf("fim");
    return 0;
}