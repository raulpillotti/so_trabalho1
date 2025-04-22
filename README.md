# Análise de dados de sensoriamento utilizando pthreads 

Este projeto implementa um analisador multithread em C, utilizando `pthreads`, para processar dados de sensores ambientais coletados por dispositivos IoT. O programa calcula estatísticas mensais (máximos, mínimos e médias) por dispositivo e sensor, e distribui o processamento entre múltiplos núcleos do sistema para melhorar o desempenho.

## Requisitos

- GCC (compilador C)
- Sistema compatível com POSIX (Linux, macOS, etc.)

## Compilação

Use o comando abaixo para compilar o programa:

```bash
  gcc -pthread main.c -o main
```

## Execução

Execute o programa com:

```bash
  ./main
```

## Formato do CSV de Entrada
O `devices.csv` deve conter dados no seguinte formato, com "|" como separador:

id|device|contagem|data|temperatura|umidade|luminosidade|ruido|eco2|etvoc|latitude|longitude
1048500|sirrosteste_UCS_AMV-17|28333|2023-04-20 06:36:49.142874|13.1|61.3|1.7|55.9|0|400|-29.161463|-51.152504

A primeira linha é o cabeçalho e será ignorada.

## Carregamento dos Dados
O arquivo `devices.csv` é lido linha a linha. Para cada linha:
- Os dados são divididos com `strtok` usando "|" como delimitador.
- Apenas registros com data a partir de março de 2024 (`2024-03`) são considerados.
- Os registros são armazenados em um vetor de structs `Record`.

## Distribuição da Carga entre as Threads
Os dados são agrupados por mês de coleta.
Cada grupo mensal é tratado por uma thread.

Exemplo:
- 2024-03 → uma thread
- 2024-04 → outra thread
- E assim por diante...

Se houver mais meses do que núcleos disponíveis, as threads são processadas em lotes, respeitando o número de núcleos disponíveis (obtido com `sysconf(_SC_NPROCESSORS_ONLN)`).

## Análise dos Dados
Cada thread:
  1. Ordena os registros por dispositivo.
  2. Para cada dispositivo, calcula:
      - Máximo, mínimo e média dos sensores:
        - Temperatura
        - Umidade
        - Luminosidade
        - Ruído
        - eCO2
        - eTVOC

  3. Os resultados são gravados em um arquivo de saída, usando mutex para evitar concorrência.

## Geração do CSV de Saída
As threads escrevem os resultados no arquivo `saida.csv`, sincronizadas com mutex (`pthread_mutex_t`) para evitar escrita simultânea.

Formato da saída:

device;data;sensor;valor_maximo;valor_medio;valor_minimo
sirrosteste_UCS_AMV-16;2024-02;Temperatura;42.20;25.15;15.80
sirrosteste_UCS_AMV-16;2024-03;Umidade;58.00;55.00;51.00

## Tipo de Threads

As threads são criadas com `pthread_create`, portanto são threads em modo usuário gerenciadas pela biblioteca pthread.

## Concorrência e Sincronização
  - As threads compartilham apenas o arquivo de saída.
  - Uso de `pthread_mutex_t` garante acesso exclusivo ao arquivo durante escrita.
  - Demais operações (leitura e análise) são feitas em memória separada por thread, evitando condições de corrida.

## Possíveis Melhorias
  - Distribuição baseada em dispositivo (além da data).
  - Análise paralela por sensor.
  - Uso de `thread pools` para controle mais eficiente.
  - Inclusão de logging detalhado.