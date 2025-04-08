#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

typedef struct {
    char device[50];
    char date[8]; // ano-mes
    float temperature;
    float humidity;
    float light;
    float noise;
    float eco2;
    float etvoc;
} Record;

typedef struct {
    char device[50];
    char year_month[8];
    char sensor[20];
    float min;
    float max;
    float sum;
    int count;
} SensorStats;

typedef struct {
    Record *records;
    int start;
    int end;
    SensorStats *stats;
    int *stats_count;
    int max_stats;
    int thread_id;
} ThreadData;

#define INITIAL_DATE "2024-03";
#define FILE_PATH "devices.csv";
#define SEPARATOR "|";

const int DATE_LEN = 8;

// Verifica se a data é >= 2024-03
int check_date(char *date) {
    int year, month;
    if (sscanf(date, "%d-%d", &year, &month) != 2) {
        return 0;
    }
    return year > 2024 || year == 2024 && month >= 3;
}

// Atualiza ou adiciona um sensor na lista
int update_sensor_stats(SensorStats *stats, int *stats_count, int max_stats, const char *device, const char *date, const char *sensor, float value) {
    for (int i = 0; i < *stats_count; i++) {
        if (strcmp(stats[i].device, device) == 0 && strcmp(stats[i].year_month, date) == 0 && strcmp(stats[i].sensor, sensor) == 0) {
            if (value < stats[i].min) stats[i].min = value;
            if (value > stats[i].max) stats[i].max = value;
            stats[i].sum += value;
            stats[i].count += 1;
            return 1;
        }
    }

    if (*stats_count >= max_stats) return 0;

    SensorStats *s = &stats[(*stats_count)++];
    strncpy(s->device, device, sizeof(s->device));
    strncpy(s->year_month, date, sizeof(s->year_month));
    strncpy(s->sensor, sensor, sizeof(s->sensor));
    s->min = s->max = s->sum = value;
    s->count = 1;

    return 1;
}

// Função da thread
void *thread_func(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    for (int i = data->start; i < data->end; i++) {
        Record r = data->records[i];
        update_sensor_stats(data->stats, data->stats_count, data->max_stats, r.device, r.date, "temperatura", r.temperature);
        update_sensor_stats(data->stats, data->stats_count, data->max_stats, r.device, r.date, "umidade", r.humidity);
        update_sensor_stats(data->stats, data->stats_count, data->max_stats, r.device, r.date, "luminosidade", r.light);
        update_sensor_stats(data->stats, data->stats_count, data->max_stats, r.device, r.date, "ruido", r.noise);
        update_sensor_stats(data->stats, data->stats_count, data->max_stats, r.device, r.date, "eco2", r.eco2);
        update_sensor_stats(data->stats, data->stats_count, data->max_stats, r.device, r.date, "etvoc", r.etvoc);
    }
    pthread_exit(NULL);
}

// Junta os resultados das threads
int merge_stats(SensorStats *final_stats, int *final_count, int max_final, SensorStats *thread_stats, int thread_count) {
    for (int i = 0; i < thread_count; i++) {
        SensorStats *s = &thread_stats[i];
        int found = 0;
        for (int j = 0; j < *final_count; j++) {
            if (strcmp(final_stats[j].device, s->device) == 0 && strcmp(final_stats[j].year_month, s->year_month) == 0 && strcmp(final_stats[j].sensor, s->sensor) == 0) {
                if (s->min < final_stats[j].min) final_stats[j].min = s->min;
                if (s->max > final_stats[j].max) final_stats[j].max = s->max;
                final_stats[j].sum += s->sum;
                final_stats[j].count += s->count;
                found = 1;
                break;
            }
        }

        if (!found) {
            if (*final_count >= max_final) return 0;
            final_stats[*final_count] = *s;
            (*final_count)++;
        }
    }

    return 1;
}

int main() {
    const char* path = FILE_PATH;
    FILE* fp = fopen(path, "r");
    if (!fp) {
        perror("Erro ao abrir o arquivo");
        return 1;
    }

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
        tk = strtok(buffer, separator); // id
        if (!tk) continue;

        tk = strtok(NULL, separator); // device
        if (!tk) continue;
        strncpy(r.device, tk, sizeof(r.device));
        r.device[sizeof(r.device) - 1] = '\0';

        tk = strtok(NULL, separator); // contagem
        if (!tk) continue;

        tk = strtok(NULL, separator); // data
        if (!tk || !check_date(tk)) continue;
        strncpy(r.date, tk, 7); 
        r.date[7] = '\0';

        tk = strtok(NULL, separator); // temperatura
        if (!tk) continue;
        r.temperature = atof(tk);

        tk = strtok(NULL, separator); // umidade
        if (!tk) continue;
        r.humidity = atof(tk);

        tk = strtok(NULL, separator); // luminosidade
        if (!tk) continue;
        r.light = atof(tk);

        tk = strtok(NULL, separator); // ruído
        if (!tk) continue;
        r.noise = atof(tk);

        tk = strtok(NULL, separator); // eco2
        if (!tk) continue;
        r.eco2 = atof(tk);

        tk = strtok(NULL, separator); // etvoc
        if (!tk) continue;
        r.etvoc = atof(tk);

        // Ignorar latitude e longitude
        records[total_count++] = r;
    }

    fclose(fp);

    // for (int i = 0; i <= 50000; i++) {
    //     printf("Device: %s | Data: %s | Temp: %.2f\n",
    //     records[i].device, records[i].date, records[i].temperature);
    // }

    int num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    pthread_t threads[num_threads];
    ThreadData thread_data[num_threads];

    int records_per_thread = total_count / num_threads;
    int extra = total_count % num_threads;
    int start = 0;

    int max_stats_per_thread = 1000;
    SensorStats **thread_stats = malloc(num_threads * sizeof(SensorStats *));
    int *stats_counts = calloc(num_threads, sizeof(int));

    for (int i = 0; i < num_threads; i++) {
      thread_stats[i] = malloc(max_stats_per_thread * sizeof(SensorStats));

      int end = start + records_per_thread + (i < extra ? 1 : 0);
      thread_data[i].records = records;
      thread_data[i].start = start;
      thread_data[i].end = end;
      thread_data[i].stats = thread_stats[i];
      thread_data[i].stats_count = &stats_counts[i];
      thread_data[i].max_stats = max_stats_per_thread;
      thread_data[i].thread_id = i;

      pthread_create(&threads[i], NULL, thread_func, &thread_data[i]);

      start = end;

    }

    for (int i = 0; i < num_threads; i++) {
      pthread_join(threads[i], NULL);
    }

    // Consolida os resultados
    int max_final_stats = 10000;
    SensorStats *final_stats = malloc(max_final_stats * sizeof(SensorStats));
    int final_stats_count = 0;

    for (int i = 0; i < num_threads; i++) {
      merge_stats(final_stats, &final_stats_count, max_final_stats, thread_stats[i], stats_counts[i]);
    }

    FILE *out = fopen("saida.csv", "w");
    fprintf(out, "device;ano-mes;sensor;valor_maximo;valor_medio;valor_minimo\n");

    for (int i = 0; i < final_stats_count; i++) {
      SensorStats s = final_stats[i];
      float media = s.sum / s.count;
      fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", s.device, s.year_month, s.sensor, s.max, media, s.min);
    }

    fclose(out);

    // Libera memória
    for (int i = 0; i < num_threads; i++) {
      free(thread_stats[i]);
    }

    free(thread_stats);
    free(stats_counts);
    free(final_stats);
    free(records);

    printf("fim");
    return 0;
}