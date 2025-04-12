#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;

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

typedef struct {
    Record *records;
    int count;
} ThreadData;

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

//função da thread
void process_record_chunk(Record *records, int count) {
    if (count == 0) return;

    float temp_min = records[0].temperature;
    float temp_max = records[0].temperature;
    float temp_sum = 0.0;

    float hum_min = records[0].humidity;
    float hum_max = records[0].humidity;
    float hum_sum = 0.0;

    float luz_min = records[0].luminosity;
    float luz_max = records[0].luminosity;
    float luz_sum = 0.0;

    float ruido_min = records[0].noise;
    float ruido_max = records[0].noise;
    float ruido_sum = 0.0;

    float eco2_min = records[0].eco2;
    float eco2_max = records[0].eco2;
    float eco2_sum = 0.0;

    float etvoc_min = records[0].etvoc;
    float etvoc_max = records[0].etvoc;
    float etvoc_sum = 0.0;

    for (int i = 0; i < count; i++) {
        Record r = records[i];

        temp_sum += r.temperature;
        if (r.temperature < temp_min) temp_min = r.temperature;
        if (r.temperature > temp_max) temp_max = r.temperature;

        hum_sum += r.humidity;
        if (r.humidity < hum_min) hum_min = r.humidity;
        if (r.humidity > hum_max) hum_max = r.humidity;

        luz_sum += r.luminosity;
        if (r.luminosity < luz_min) luz_min = r.luminosity;
        if (r.luminosity > luz_max) luz_max = r.luminosity;

        ruido_sum += r.noise;
        if (r.noise < ruido_min) ruido_min = r.noise;
        if (r.noise > ruido_max) ruido_max = r.noise;

        eco2_sum += r.eco2;
        if (r.eco2 < eco2_min) eco2_min = r.eco2;
        if (r.eco2 > eco2_max) eco2_max = r.eco2;

        etvoc_sum += r.etvoc;
        if (r.etvoc < etvoc_min) etvoc_min = r.etvoc;
        if (r.etvoc > etvoc_max) etvoc_max = r.etvoc;
    }

    float temp_media = temp_sum / count;
    float hum_media = hum_sum / count;
    float luz_media = luz_sum / count;
    float ruido_media = ruido_sum / count;
    float eco2_media = eco2_sum / count;
    float etvoc_media = etvoc_sum / count;

    pthread_mutex_lock(&write_mutex);

    FILE* out = fopen("saida.csv", "a");
    if (out) {
        fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", records[0].device, records[0].date, "Temperatura", temp_max, temp_media, temp_min);
        fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", records[0].device, records[0].date, "Umidade", hum_max, hum_media, hum_min);
        fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", records[0].device, records[0].date, "Luminosidade", luz_max, luz_media, luz_min);
        fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", records[0].device, records[0].date, "Ruído", ruido_max, ruido_media, ruido_min);
        fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", records[0].device, records[0].date, "eCO2", eco2_max, eco2_media, eco2_min);
        fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", records[0].device, records[0].date, "eTVOC", etvoc_max, etvoc_media, etvoc_min);
        fclose(out);
    } else {
        perror("Erro ao abrir arquivo de saída");
    }

    pthread_mutex_unlock(&write_mutex);
}

void* thread_function(void* arg) {
    ThreadData* data = (ThreadData*) arg;
    process_record_chunk(data->records, data->count);
    free(data);
    return NULL;
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

    // for (int i = 0; i < total_count && i < 5; i++) {
    //     printf("Registro %d:\n", i + 1);
    //     printf("  Device      : %s\n", records[i].device);
    //     printf("  Data        : %s\n", records[i].date);
    //     printf("  Temperatura : %.2f\n", records[i].temperature);
    //     printf("  Umidade     : %.2f\n", records[i].humidity);
    //     printf("  Luminosidade: %.2f\n", records[i].luminosity);
    //     printf("  Ruído       : %.2f\n", records[i].noise);
    //     printf("  eCO2        : %.2f\n", records[i].eco2);
    //     printf("  eTVOC       : %.2f\n", records[i].etvoc);
    //     printf("\n");
    // }

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

    int max_threads = sysconf(_SC_NPROCESSORS_ONLN);
    pthread_t threads[chunk_count];

    FILE* out = fopen("saida.csv", "w");
    fprintf(out, "device;data;sensor;valor_maximo;valor_medio;valor_minimo\n");
    fclose(out);

    int cur_chunk = 0;
    int limit = cur_chunk + max_threads;

    while (cur_chunk < chunk_count) {
        for (int i = cur_chunk; i < limit; i++) {
            ThreadData *data = malloc(sizeof(ThreadData));
            data->records = records_by_date[i];
            data->count = chunk_counts[i];

            if (pthread_create(&threads[i], NULL, thread_function, data) != 0) {
                perror("Erro ao criar thread");
                return 1;
            }
        }

        for (int i = cur_chunk; i < limit; i++) {
            pthread_join(threads[i], NULL);
            cur_chunk ++;
        }
    }

    // process_record_chunk(records_by_date[0], chunk_counts[0]);
    printf("fim");
    return 0;
}