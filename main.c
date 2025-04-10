#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char device[50];        
    char date[10];          
    float temperature;      
    float humidity;
    float luminosity;
    float noise;
    float eco2;
    float etvoc;
} Record;

#define INITIAL_DATE "2024-03";
#define FILE_PATH "devices.csv";
#define SEPARATOR "|";

const int DATE_LEN = 8;

int check_date(char *date) {
    int year, month;
    if (sscanf(date, "%d-%d", &year, &month) != 2) {
        return 0;
    }
    return year > 2024 || year == 2024 && month >= 3;
}

void process_record_chunk(Record *records, int count) {
}

int main() {
    const char* path = FILE_PATH;
    FILE* fp = fopen(path, "r");
    int total_count = 0;
    int capacity = 1000;
    Record *records = malloc(capacity * sizeof(Record));

    char buffer[1024];
    fgets(buffer, sizeof(buffer), fp);
    const char* separator = SEPARATOR;

    while (fgets(buffer, sizeof(buffer), fp)) {
        if (total_count >= capacity) {
            capacity *= 2;
            records = realloc(records, capacity * sizeof(Record));
        }

        Record r;
        char *tk;

        tk = strtok(buffer, separator);
        if (!tk) continue;

        tk = strtok(NULL, separator);
        if (!tk) continue;
        strncpy(r.device, tk, sizeof(r.device));
        r.device[sizeof(r.device) - 1] = '\0';

        tk = strtok(NULL, separator);
        if (!tk) continue;

        tk = strtok(NULL, separator);
        if (!tk || !check_date(tk)) continue;
        strncpy(r.date, tk, 7); 
        r.date[7] = '\0';

        tk = strtok(NULL, separator);
        if (!tk) continue;
        r.temperature = atof(tk);

        tk = strtok(NULL, separator);
        if (!tk) continue;
        r.humidity = atof(tk);

        tk = strtok(NULL, separator);
        if (!tk) continue;
        r.luminosity = atof(tk);

        tk = strtok(NULL, separator);
        if (!tk) continue;
        r.noise = atof(tk);

        tk = strtok(NULL, separator);
        if (!tk) continue;
        r.eco2 = atof(tk);


        tk = strtok(NULL, separator);
        if (!tk) continue;
        r.etvoc = atof(tk);

        records[total_count++] = r;
    }

    fclose(fp);

    for (int i = 0; i < total_count && i < 5; i++) {
        printf("Registro %d:\n", i + 1);
        printf("  Device      : %s\n", records[i].device);
        printf("  Data        : %s\n", records[i].date);
        printf("  Temperatura : %.2f\n", records[i].temperature);
        printf("  Umidade     : %.2f\n", records[i].humidity);
        printf("  Luminosidade: %.2f\n", records[i].luminosity);
        printf("  Ruído       : %.2f\n", records[i].noise);
        printf("  eCO2        : %.2f\n", records[i].eco2);
        printf("  eTVOC       : %.2f\n", records[i].etvoc);
        printf("\n");
    }

    // for (int i = 0; i <= 50000; i++) {
    //     printf("Device: %s | Data: %s | Temp: %.2f\n",
    //     records[i].device, records[i].date, records[i].temperature);
    // }

    Record *records_by_date[50];
    int chunk_index = 0;
    int chunk_counts[50];
    int chunk_counts_index = 0;

   char cur_date[8 + 1];  // 7 caracteres + \0
    strncpy(cur_date, records[0].date, 7);
    cur_date[7] = '\0';

    records_by_date[0] = &records[0];
    chunk_counts[0] = 1;
    chunk_index = 1;

    for (int i = 1; i < total_count; i++) {
        char date[8 + 1];
        strncpy(date, records[i].date, 7);
        date[7] = '\0';  

        if (strcmp(cur_date, date) != 0) {
            strncpy(cur_date, date, 7);
            cur_date[7] = '\0'; 
            
            records_by_date[chunk_index] = &records[i];
            chunk_counts[chunk_index] = 1;
            chunk_index++;
        } else {
            chunk_counts[chunk_index - 1]++;
        }
    }
    int chunk_count = chunk_index;
 
    for (int i = 0; i < 50; i ++) {
        printf("count %d", chunk_counts[i]);
    }
    printf("fim");
    return 0;
}