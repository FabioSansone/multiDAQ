#ifndef WORKER_DISPATCHER_H
#define WORKER_DISPATCHER_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

// L'idea è che se io ho 1000 tank e ho 4 worker che lavorano in parallelo per ricevere e processare i dati, ognuno di loro si becca circa 250 tank.
// Per cercare velocemente non creo una lista con 250 elementi ma creo differenti "cassetti" => i bucket.
// Ogni worker ha la sua table e io con un operazione di modulo individuo il cassetto in cui cercare che non avrà 250 tank (=nodi) ma di meno in base alla hash function

typedef struct
{
    uint32_t version;
    int64_t timestamp_raw;

    char timestamp_utc[32];
    char acquisition_mode[16];
    char acquisition_type[32];
    char acq_type_param[32];

    int32_t status_reg0;
    int32_t status_reg1;
    int32_t status_reg10;
    int32_t status_reg15;
    int32_t status_reg16;
    int32_t status_reg18;
    int32_t status_reg19;
    int32_t status_reg31;
    int32_t status_reg39;

    char serial_pmt[7][32];
    char client_id[32];

} meta_data_t;


typedef struct node
{   
    meta_data_t metadata;
    int output_format;          //0 per CSV, 1 per bin
    int chunk_index;            //per il rollover, parte da 0
    uint64_t bytes_written;     //per tenere traccia della dimensione del file
    char *source_id;            // identità della tank
    FILE *output_file;          // puntatore al file aperto per questa tank
    char file_path[256];        // percorso verso il file
    char base_file_path[256];   // nome originale, MAI modificato dal rollover
    uint64_t events_written;    // contatore
    struct node *next;          // per gestire le collisioni nel bucket
    char write_buffer[1024 * 128]; // 128KB buffer on diske to store fixed amount of data before flushing on file
    size_t buffer_used;             //index of the buffer

} tank_node;

typedef struct table
{
    size_t num_buckets;  // quanti bucket ha QUESTO worker
    tank_node **buckets;
} tank_table;



/**
 * Compute the 64-bit FNV-1a hash of a byte sequence.
 *
 * data:
 *     Pointer to the bytes to hash.
 *
 * len_data:
 *     Number of bytes to hash.
 *
 * Returns:
 *     The 64-bit FNV-1a hash.
 */
uint64_t hash_fnv1a(const unsigned char *data, size_t len_data);

tank_table *create_table(size_t num_buckets);

tank_node *create_node(const char *source_id, FILE *output_file, meta_data_t metadata, int output_format, char *file_path);

size_t key_index(const unsigned char *key, size_t key_len, size_t num_buckets);

int add_node(tank_table *table, const char *source_id, FILE *output_file, meta_data_t metadata, int output_format, char *file_path);

tank_node *get_node(const tank_table *table, const char *source_id);

void delete_table(tank_table *table);



#endif