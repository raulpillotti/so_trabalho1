#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

// Mutex para garantir escrita segura no arquivo por múltiplas threads
pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;

// Estrutura que representa um registro de sensores
typedef struct {
    char device[50];        // Nome do dispositivo
    char date[10];          // Data da leitura (yyyy-mm)         
    float temperature;      
    float humidity;
    float luminosity;
    float noise;
    float eco2;
    float etvoc;
} Record;

// Dados que serão passados para cada thread
typedef struct {
    Record *records;  // Ponteiro para os registros do chunk
    int count;        // Quantidade de registros
} ThreadData;

#define INITIAL_DATE "2024-03";
#define FILE_PATH "devices.csv";
#define SEPARATOR "|";

const int DATE_LEN = 8;

// Verifica se a data é igual ou posterior a março de 2024
int check_date(char *date) {
    int year, month;
    if (sscanf(date, "%d-%d", &year, &month) != 2) {
        return 0;
    }
    return year > 2024 || year == 2024 && month >= 3;
}

// Função usada para ordenar os registros por nome do dispositivo
int sort_by_device(const void *a, const void *b) {
    const Record *ra = (Record*)a;
    const Record *rb = (Record*)b;
    return strcmp(ra->device, rb->device);
}

//função da thread
void process_record_chunk(Record *records, int count) {
    if (count == 0) return;

    // Ordena por nome do dispositivo
    qsort(records, count, sizeof(Record), sort_by_device);

    // Inicializa com o primeiro registro
    char current_device[50];
    strncpy(current_device, records[0].device, 50); 
    current_device[49] = '\0'; 

    // Inicializa variáveis para o cálculo estatístico
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

    int record_count_by_device = 0;

    // Percorre registros, agrupando por dispositivo
    for (int i = 1; i <= count; i ++) {
        Record r = records[i];

        // Novo dispositivo detectado
        if (i == count || strcmp(current_device, r.device) != 0) {

            // Calcula médias para o dispositivo anterior
            float temp_media = temp_sum / record_count_by_device;
            float hum_media = hum_sum / record_count_by_device;
            float luz_media = luz_sum / record_count_by_device;
            float ruido_media = ruido_sum / record_count_by_device;
            float eco2_media = eco2_sum / record_count_by_device;
            float etvoc_media = etvoc_sum / record_count_by_device;

            // Escreve resultados no arquivo (com mutex para evitar concorrência)
            pthread_mutex_lock(&write_mutex);

            FILE* out = fopen("saida.csv", "a");
            if (out) {
                fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", r.device, r.date, "Temperatura", temp_max, temp_media, temp_min);
                fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", r.device, r.date, "Umidade", hum_max, hum_media, hum_min);
                fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", r.device, r.date, "Luminosidade", luz_max, luz_media, luz_min);
                fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", r.device, r.date, "Ruído", ruido_max, ruido_media, ruido_min);
                fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", r.device, r.date, "eCO2", eco2_max, eco2_media, eco2_min);
                fprintf(out, "%s;%s;%s;%.2f;%.2f;%.2f\n", r.device, r.date, "eTVOC", etvoc_max, etvoc_media, etvoc_min);
                fclose(out);
            } else {
                perror("Erro ao abrir arquivo de saída");
            }

            pthread_mutex_unlock(&write_mutex);

            strncpy(current_device, r.device, 50);
            current_device[49] = '\0'; 

            temp_min = r.temperature;
            temp_max = r.temperature;
            temp_sum = 0.0;

            hum_min = r.humidity;
            hum_max = r.humidity;
            hum_sum = 0.0;

            luz_min = r.luminosity;
            luz_max = r.luminosity;
            luz_sum = 0.0;

            ruido_min = r.noise;
            ruido_max = r.noise;
            ruido_sum = 0.0;

            eco2_min = r.eco2;
            eco2_max = r.eco2;
            eco2_sum = 0.0;

            etvoc_min = r.etvoc;
            etvoc_max = r.etvoc;
            etvoc_sum = 0.0;

            record_count_by_device = 0;

        } else {
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

            record_count_by_device++;
        }
    }
}

// Função executada por cada thread
void* thread_function(void* arg) {
    ThreadData* data = (ThreadData*) arg;
    process_record_chunk(data->records, data->count);
    free(data);
    return NULL;
}

int main() {
    const char* path = FILE_PATH;
    FILE* fp = fopen(path, "r");

    // Leitura dos registros do arquivo
    int total_count = 0;
    int capacity = 1000;
    Record *records = malloc(capacity * sizeof(Record));

    char buffer[1024];
    fgets(buffer, sizeof(buffer), fp); // Pula cabeçalho
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

    // Agrupa registros por data
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

    // Número de threads baseado na quantidade de núcleos da CPU
    int MAX_THREADS = sysconf(_SC_NPROCESSORS_ONLN);
    int num_threads = chunk_count;
    pthread_t threads[num_threads];

    // Cria o arquivo de saída com cabeçalho
    FILE* out = fopen("saida.csv", "w");
    fprintf(out, "device;data;sensor;valor_maximo;valor_medio;valor_minimo\n");
    fclose(out);

    int cur_chunk = 0;
    while (cur_chunk < chunk_count) {
        for (int i = cur_chunk; i < cur_chunk + MAX_THREADS; i++) {
            ThreadData *data = malloc(sizeof(ThreadData));
            data->records = records_by_date[i];
            data->count = chunk_counts[i];

            if (pthread_create(&threads[i], NULL, thread_function, data) != 0) {
                perror("Erro ao criar thread");
                return 1;
            }
        }

        for (int i = cur_chunk; i < cur_chunk + MAX_THREADS; i++) {
            pthread_join(threads[i], NULL);
        }
        
        cur_chunk += MAX_THREADS;
    }

    printf("fim \n");
    return 0;
}