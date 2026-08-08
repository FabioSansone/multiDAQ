#include <stdio.h>
#include <string.h>
#include <zmq.h>
#include <assert.h>
#include <pthread.h>
#include  <signal.h>
#include <time.h>
#include <unistd.h> 
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include "dispatch/worker_dispatcher.h"



#define EVENT_SIZE_WORDS 8  //8 parole da 16 bit => dimensione della parola dal DMA
#define EVENT_SIZE_BYTES 16 //16 byte per evento
#define QUEUE_SIZE 16384 //Numero di eventi che mi aspetto da evreceiver
#define PROCESS_BATCH_SIZE 2048 //Quanti eventi prelevare dalla coda in un sol colpa per analizzarli e scriverli

#define NUM_EVENTS_WRITE 100 //How much events are periodically written

#define MAX_SOURCE_ID_LEN 32 //Size in byte of the source id

#define N_WORKERS 4 //Number of parallel workers for processing the data
#define  NUM_BUCKETS_PER_WORKER 64 //The number of linked lists in each table for each worker node

#define FILE_HEADER_SIZE 512

#define MAX_FILE_SIZE_BYTES (500UL * 1024 * 1024)

static volatile sig_atomic_t keep_running = 1;

typedef enum {ITEM_DATA, ITEM_OPEN, ITEM_CLOSE} item_type_t;


//definisco il singolo evento (array di 8 parole da 16 bit)
typedef struct {
    uint16_t words[EVENT_SIZE_WORDS];
    int valid;
    char source_id[MAX_SOURCE_ID_LEN]; //popolato dall'arrivo della word dal DMA per taggare i dati
} event_t;




typedef struct __attribute__((packed))
{
    char magic[8];

    uint32_t version;
    uint32_t header_size;

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

    uint8_t reserved[84];

} file_header_t;

_Static_assert(
    sizeof(file_header_t) == FILE_HEADER_SIZE,
    "file_header_t must be exactly 512 bytes"
);


//wrapper per event_t per poter differenziare comando e dato e per poter dare il percorso del file

typedef struct 
{
    item_type_t item;
    event_t data;                       // valido solo se type == ITEM_DATA
    char source_id[MAX_SOURCE_ID_LEN]; // per OPEN/CLOSE; popolato dal servizio DataReceiver in Python per taggare le operazioni su uno specifico client
    char path[256];                     // valido solo se type == ITEM_OPEN
    meta_data_t metainfo;
    int output_format;
}queue_item_t;




typedef struct 
{
    queue_item_t items[QUEUE_SIZE]; 
    int head;
    int tail;
    int count;
    tank_table *worker_table;
    pthread_mutex_t lock;
    pthread_cond_t data_available;
    pthread_cond_t space_available;
} worker_node_t;

typedef struct 
{
    int worked_idx;
}worker_args_t;


worker_node_t worker_nodes[N_WORKERS];



typedef struct __attribute__((packed)) {
    uint8_t  canale;
    uint16_t tempo_16_bit;
    uint32_t coarse_time;
    uint8_t  tdc_time;
    uint8_t  tot;
    uint8_t  tdc_trigger_end;
    uint16_t energia;
} event_record_t;

//event_t event_queue[QUEUE_SIZE]; //array di 4096 strutture di tipo event_t per ospitare tutte le parole

// int queue_head = 0;
// int queue_tail = 0;
// int queue_count = 0;



// pthread_mutex_t lock;
// pthread_cond_t data_available  = PTHREAD_COND_INITIALIZER;
// pthread_cond_t space_available = PTHREAD_COND_INITIALIZER;

//pthread_mutex_t file_write_lock = PTHREAD_MUTEX_INITIALIZER;


void reset_worker_node(worker_node_t *node) {
    node->head = 0;
    node->tail = 0;
    node->count = 0;
    pthread_mutex_init(&node->lock, NULL);
    pthread_cond_init(&node->data_available, NULL);
    pthread_cond_init(&node->space_available, NULL);
    node->worker_table = create_table(NUM_BUCKETS_PER_WORKER);
    if (node->worker_table == NULL) {
        printf("ERROR: failed to create tank_table for worker\n");
    }
}

void reset_meta_struct(meta_data_t *meta){
    meta->version = 0;
    meta->timestamp_raw = 0;
    meta->timestamp_utc[0] = '\0';
    meta->acquisition_mode[0] = '\0';
    meta->acquisition_type[0] = '\0';
    meta->acq_type_param[0] = '\0';   
    meta->status_reg0 = 0;
    meta->status_reg1 = 0;
    meta->status_reg10 = 0;
    meta->status_reg15 = 0;
    meta->status_reg16 = 0;
    meta->status_reg18 = 0;
    meta->status_reg19 = 0;
    meta->status_reg31 = 0;
    meta->status_reg39 = 0;
    for (int ch = 0; ch < 7; ch++) {
        meta->serial_pmt[ch][0] = '\0';
    }
    meta->client_id[0] = '\0';
}

void populate_meta_struct(meta_data_t *meta, char *meta_buff) {
    reset_meta_struct(meta);

    char *outer_save = NULL;
    char *token = strtok_r(meta_buff, ";", &outer_save);

    while (token != NULL) {
        char *inner_save = NULL;
        char *key = strtok_r(token, "=", &inner_save);
        char *value = strtok_r(NULL, "=", &inner_save);

        if (key != NULL && value != NULL) {
            if (strcmp(key, "version") == 0) {
                meta->version = atoi(value);
            } else if (strcmp(key, "timestamp_raw") == 0) {
                meta->timestamp_raw = atol(value);
            } else if (strcmp(key, "timestamp_utc") == 0) {
                strncpy(meta->timestamp_utc, value, sizeof(meta->timestamp_utc) - 1);
                meta->timestamp_utc[sizeof(meta->timestamp_utc) - 1] = '\0';
            } else if (strcmp(key, "acquisition_mode") == 0) {
                strncpy(meta->acquisition_mode, value, sizeof(meta->acquisition_mode) - 1);
                meta->acquisition_mode[sizeof(meta->acquisition_mode) - 1] = '\0';
            } else if (strcmp(key, "acquisition_type") == 0) {
                strncpy(meta->acquisition_type, value, sizeof(meta->acquisition_type) - 1);
                meta->acquisition_type[sizeof(meta->acquisition_type) - 1] = '\0';
            } else if (strcmp(key, "status_reg0") == 0) {
                meta->status_reg0 = atoi(value);
            } else if (strcmp(key, "status_reg1") == 0) {
                meta->status_reg1 = atoi(value);
            } else if (strcmp(key, "status_reg10") == 0) {
                meta->status_reg10 = atoi(value);
            } else if (strcmp(key, "status_reg15") == 0) {
                meta->status_reg15 = atoi(value);
            } else if (strcmp(key, "status_reg16") == 0) {
                meta->status_reg16 = atoi(value);
            } else if (strcmp(key, "status_reg18") == 0) {
                meta->status_reg18 = atoi(value);
            } else if (strcmp(key, "status_reg19") == 0) {
                meta->status_reg19 = atoi(value);
            } else if (strcmp(key, "status_reg31") == 0) {
                meta->status_reg31 = atoi(value);
            } else if (strcmp(key, "status_reg39") == 0) {
                meta->status_reg39 = atoi(value);
            } else if (strncmp(key, "serial_pmt", 10) == 0 && strlen(key) == 11) {
                int ch = key[10] - '0';
                if (ch >= 0 && ch < 7) {
                    strncpy(meta->serial_pmt[ch], value, sizeof(meta->serial_pmt[ch]) - 1);
                    meta->serial_pmt[ch][sizeof(meta->serial_pmt[ch]) - 1] = '\0';
                } 
            } else if (strcmp(key, "client_id") == 0) {
                strncpy(meta->client_id, value, sizeof(meta->client_id) - 1);
                meta->client_id[sizeof(meta->client_id) - 1] = '\0';
            } else if (strcmp(key, "acq_type_param") == 0) {
                strncpy(meta->acq_type_param, value, sizeof(meta->acq_type_param) - 1);
                meta->acq_type_param[sizeof(meta->acq_type_param) - 1] = '\0';
            }
        }

        token = strtok_r(NULL, ";", &outer_save);
    }
}

int write_header(FILE *file, const meta_data_t *meta)
{
    file_header_t header = {0};

    memcpy(header.magic, "MDAQH01", 7);

    header.version = meta->version;
    header.header_size = FILE_HEADER_SIZE;

    header.timestamp_raw = meta->timestamp_raw;

    snprintf(
        header.timestamp_utc,
        sizeof(header.timestamp_utc),
        "%s",
        meta->timestamp_utc
    );

    snprintf(
        header.acquisition_mode,
        sizeof(header.acquisition_mode),
        "%s",
        meta->acquisition_mode
    );

    snprintf(
        header.acquisition_type,
        sizeof(header.acquisition_type),
        "%s",
        meta->acquisition_type
    );

    snprintf(
        header.acq_type_param,
        sizeof(header.acq_type_param),
        "%s",
        meta->acq_type_param
    );

    header.status_reg0  = meta->status_reg0;
    header.status_reg1  = meta->status_reg1;
    header.status_reg10 = meta->status_reg10;
    header.status_reg15 = meta->status_reg15;
    header.status_reg16 = meta->status_reg16;
    header.status_reg18 = meta->status_reg18;
    header.status_reg19 = meta->status_reg19;
    header.status_reg31 = meta->status_reg31;
    header.status_reg39 = meta->status_reg39;

    for (size_t ch = 0; ch < 7; ++ch) {
        snprintf(
            header.serial_pmt[ch],
            sizeof(header.serial_pmt[ch]),
            "%s",
            meta->serial_pmt[ch]
        );
    }

    snprintf(
        header.client_id,
        sizeof(header.client_id),
        "%s",
        meta->client_id
    );

    size_t written = fwrite(
        &header,
        1,
        sizeof(header),
        file
    );

    if (written != sizeof(header)) {
        return -1;
    }

    return 0;
}


static void sig_handler(int _)
{
    (void)_;
    keep_running = 0;
}


void rollover_file(tank_node *node)
{
    fclose(node->output_file);
    node->output_file = NULL;

    node->chunk_index++;

    char new_path[512];

    const char *ext = strrchr(node->base_file_path, '.');

    if (ext != NULL) {

        size_t stem_len = (size_t)(ext - node->base_file_path);
        const char *chunk_tag = "_chunk";
        const size_t chunk_suffix_len = 9;   // "_chunk000"

        size_t base_len = stem_len;

        if (stem_len >= chunk_suffix_len && strncmp(node->base_file_path + stem_len - chunk_suffix_len, chunk_tag, strlen(chunk_tag)) == 0) {
            base_len = stem_len - chunk_suffix_len;
        }

        snprintf(new_path, sizeof(new_path), "%.*s_chunk%03d%s", (int)base_len, node->base_file_path, node->chunk_index, ext);

    } else {
        snprintf(new_path, sizeof(new_path), "%s_chunk%03d", node->base_file_path, node->chunk_index);
    }

    FILE *new_file = fopen(new_path, "w");

    if (new_file == NULL) {
        printf("ERROR: rollover failed to open '%s' for tank '%s'\n", new_path, node->source_id);
        return;
    }

    if (write_header(new_file, &node->metadata) != 0) {
        printf("ERROR: rollover failed to write header for tank '%s'\n", node->source_id);
        fclose(new_file);
        return;
    }

    if (node->output_format == 0) {
        fprintf(new_file, "Channel,Unix_time_16_bit,Coarse_time," "TDC_time,ToT_time,TDC_trigger_end,Energy\n");
        fflush(new_file);
    }

    node->output_file = new_file;
    node->bytes_written = 0;

    strncpy(node->file_path, new_path, sizeof(node->file_path) - 1);

    node->file_path[sizeof(node->file_path) - 1] = '\0';

    printf("Rollover: tank '%s' -> '%s' (chunk %d)\n", node->source_id, new_path, node->chunk_index);
}


uint32_t get_bits(const uint16_t *buffer, size_t bit_offset, size_t num_bits){


    uint32_t result = 0;

    for(size_t i = 0; i < num_bits; i++){

        size_t current_bit = bit_offset + i;
        size_t word_index = current_bit / 16;
        //Con il modulo determino l'indice del bit all'interno della parola. E' come se riscalassi gli indici ad un intevrallo compreso tra 0 e 15 
        //Il 15 - mi serve per traslare visto che non posso leggere da 0 a 15 come se fosse una lista ma da 15 a 0 (prima MSB)
        size_t bit_in_word = 15 - (current_bit % 16);

        uint16_t  word_16 = buffer[word_index];
        //Data la parola sposto tutto a destra di quello che serve, conoscendo l'indice del bit nella lista, per poi prendermelo come ultimo elemento
        uint32_t  bit_value = (word_16 >> bit_in_word) & 1;

        result = (result << 1) | bit_value;


    }


    return result;

}

int check_crc(const uint16_t *buffer) {
    uint8_t crc_fpga = get_bits(buffer, 88, 8); 
    uint8_t crc_calc = 0;
 
    for (size_t byte_idx = 0; byte_idx < 11; byte_idx++) {
        uint8_t byte_val = get_bits(buffer, byte_idx * 8, 8);
        crc_calc ^= byte_val;
    }

    return (crc_fpga == crc_calc) ? 0 : 1;
}



/*
void *run_control(void *args){ 
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL); 
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL); 
    
    void *context_rc = zmq_ctx_new (); 
    assert(context_rc != NULL); 
    
    void *rc_socket = zmq_socket (context_rc, ZMQ_PUB); 
    assert(rc_socket != NULL); 
    
    int check_rc_bind = zmq_bind(rc_socket, "tcp://*:4444"); 
    if (check_rc_bind != 0){ 
        printf("Bind Error: %s\n", zmq_strerror(zmq_errno())); 
        return NULL; 
    } 
    printf("RC binded on port 4444\n"); 
    
    sleep(1); 
    
    zmq_send(rc_socket, "control", 7, ZMQ_SNDMORE); 
    zmq_send(rc_socket, "start", 5, 0); 
    printf("Sent START message (topic: control)\n"); 
    
    while(keep_running){ 
        sleep(1); 
    } 
    
    zmq_send(rc_socket, "control", 7, ZMQ_SNDMORE); 
    zmq_send(rc_socket, "stop", 4, 0); 
    printf("Sent STOP message\n"); 
    zmq_close(rc_socket); 
    zmq_ctx_destroy(context_rc); 
    
    return NULL; 
}

*/

void *control_listener(void *args){
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL); 
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL); 

    void *context_cl = zmq_ctx_new (); 
    assert(context_cl != NULL); 
    
    void *cl_socket = zmq_socket (context_cl, ZMQ_REP); //Riceve le richieste dal Python
    assert(cl_socket != NULL); 
    
    int check_cl_connect  = zmq_connect(cl_socket, "tcp://localhost:5556"); 
    if (check_cl_connect  != 0){ 
        printf("Connect Error: %s\n", zmq_strerror(zmq_errno())); 
        return NULL; 
    } 
    printf("CL connected  on port 5556\n");


    void *context_rc = zmq_ctx_new (); 
    assert(context_rc != NULL); 
    
    void *rc_socket = zmq_socket (context_rc, ZMQ_PUB); 
    assert(rc_socket != NULL); 
    
    int check_rc_bind = zmq_bind(rc_socket, "tcp://*:4444"); 
    if (check_rc_bind != 0){ 
        printf("Bind Error: %s\n", zmq_strerror(zmq_errno())); 
        return NULL; 
    } 
    printf("RC binded on port 4444\n"); 
    
    sleep(1); 

    while (keep_running) {
        zmq_pollitem_t poll_items[] = { { cl_socket, 0, ZMQ_POLLIN, 0 } };
        if (zmq_poll(poll_items, 1, 500) <= 0)
            continue;
        if (!(poll_items[0].revents & ZMQ_POLLIN))
            continue;

        char command[16] = {0};
        char id_str[MAX_SOURCE_ID_LEN] = {0};
        char path[256] = {0};
        char format_file_str[8] = {0};
        char metadata[512] = {0};

        int frame_idx = 0;
        int more = 0;
        size_t more_size = sizeof(more);

        do {
            zmq_msg_t msg;
            zmq_msg_init(&msg);
            if (zmq_msg_recv(&msg, cl_socket, 0) == -1) {
                zmq_msg_close(&msg);
                break;
            }

            size_t frame_size = zmq_msg_size(&msg);
            const char *frame_data = (const char *) zmq_msg_data(&msg);

            char *dest = NULL;
            size_t dest_cap = 0;
            if (frame_idx == 0)      { dest = command;  dest_cap = sizeof(command); }
            else if (frame_idx == 1) { dest = id_str;   dest_cap = sizeof(id_str); }
            else if (frame_idx == 2) { dest = path;     dest_cap = sizeof(path); }
            else if (frame_idx == 3) { dest = format_file_str; dest_cap = sizeof(format_file_str);}
            else if (frame_idx == 4) { dest = metadata; dest_cap = sizeof(metadata); }

            if (dest != NULL) {
                size_t len = (frame_size < dest_cap - 1) ? frame_size : dest_cap - 1;
                memcpy(dest, frame_data, len);
                dest[len] = '\0';
            }

            zmq_getsockopt(cl_socket, ZMQ_RCVMORE, &more, &more_size);
            zmq_msg_close(&msg);
            frame_idx++;

        } while (more);

        int n_matched = frame_idx;   

        const char *reply_msg = NULL;

        if (strcmp(command, "OPEN") == 0 && n_matched < 3) {
            reply_msg = "ERROR: OPEN parsing failed";
        } else if (strcmp(command, "CLOSE") == 0 && n_matched < 2) {
            reply_msg = "ERROR: CLOSE parsing failed";
        } else if (strcmp(command, "OPEN") != 0 && strcmp(command, "CLOSE") != 0) {
            reply_msg = "ERROR: command not recognized";
        }

        if (reply_msg != NULL) {
            zmq_send(cl_socket, reply_msg, strlen(reply_msg), 0);
            continue;  
        }

        uint64_t h = hash_fnv1a((const unsigned char *)id_str, strlen(id_str));
        int worker_idx = (int)(h % N_WORKERS);
        worker_node_t *wn = &worker_nodes[worker_idx];

        pthread_mutex_lock(&wn->lock);

        while (wn->count >= QUEUE_SIZE && keep_running) {
            pthread_cond_wait(&wn->space_available, &wn->lock);
        }

        if (!keep_running) {
            pthread_mutex_unlock(&wn->lock);
            break;
        }

        queue_item_t command_event;
        meta_data_t meta;

        if (strcmp(command, "OPEN") == 0) {
            if (metadata[0] != '\0') {
                populate_meta_struct(&meta, metadata);
            } else {
                reset_meta_struct(&meta);
            }

            command_event.item = ITEM_OPEN;
            strncpy(command_event.source_id, id_str, MAX_SOURCE_ID_LEN - 1);
            command_event.source_id[MAX_SOURCE_ID_LEN - 1] = '\0';
            strncpy(command_event.path, path, sizeof(command_event.path) - 1);
            command_event.path[sizeof(command_event.path) - 1] = '\0';
            command_event.metainfo = meta;
            command_event.output_format = 0;

            if (format_file_str[0] != '\0'){
                if (strcmp(format_file_str, "bin") == 0){
                    command_event.output_format = 1;
                }
                else if (strcmp(format_file_str, "csv") == 0) {
                    command_event.output_format = 0;
                }
                else{
                    printf("Output file format not recognized. Usign csv as default");
                    command_event.output_format = 0;
                }
            }

            //printf("DEBUG: about to send start to id='%s' (len=%zu)\n", id_str, strlen(id_str));
            char topic_buf[MAX_SOURCE_ID_LEN + 1];
            snprintf(topic_buf, sizeof(topic_buf), "%s|", id_str);
            zmq_send(rc_socket, topic_buf, strlen(topic_buf), ZMQ_SNDMORE); 
            zmq_send(rc_socket, "start", 5, 0); 
            printf("Sent START message to id='%s'\n", id_str);

        } else {

            char topic_buf[MAX_SOURCE_ID_LEN + 1];
            snprintf(topic_buf, sizeof(topic_buf), "%s|", id_str);
            zmq_send(rc_socket, topic_buf, strlen(topic_buf), ZMQ_SNDMORE); 
            zmq_send(rc_socket, "stop", 4, 0); 
            printf("Sent STOP message to id='%s'\n", id_str);

            sleep(2);

            command_event.item = ITEM_CLOSE;
            strncpy(command_event.source_id, id_str, MAX_SOURCE_ID_LEN - 1);
            command_event.source_id[MAX_SOURCE_ID_LEN - 1] = '\0';

            

        }

        wn->items[wn->tail] = command_event;
        wn->tail = (wn->tail + 1) % QUEUE_SIZE;
        wn->count++;

        pthread_cond_signal(&wn->data_available);
        pthread_mutex_unlock(&wn->lock);

        zmq_send(cl_socket, "OK", 2, 0);
    }

    zmq_close(cl_socket); 
    zmq_ctx_destroy(context_cl); 

    zmq_close(rc_socket); 
    zmq_ctx_destroy(context_rc); 
    
    return NULL; 
    
}


void *receive_data(void *args) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    void *context = zmq_ctx_new();
    void *server_socket = zmq_socket(context, ZMQ_ROUTER);

    if (zmq_bind(server_socket, "tcp://*:5555") != 0) {
        printf("ERROR binding: %s\n", zmq_strerror(zmq_errno()));
        zmq_close(server_socket);
        zmq_ctx_destroy(context);
        return NULL;
    }

    printf("Server binded on port 5555\n");

    while (keep_running) {

        zmq_pollitem_t items[] = {
            { server_socket, 0, ZMQ_POLLIN, 0 }
        };

        if (zmq_poll(items, 1, 500) <= 0) //500 ms timeout
            continue;

        if (!(items[0].revents & ZMQ_POLLIN))
            continue;

        int more = 0;
        size_t more_size = sizeof(more);
        int frame_idx = 0;

        unsigned char *payload = NULL;
        size_t payload_size = 0;

        unsigned char identity_buf[MAX_SOURCE_ID_LEN];
        size_t identity_len = 0;

        do {
            zmq_msg_t msg;
            zmq_msg_init(&msg);

            if (zmq_msg_recv(&msg, server_socket, 0) == -1) {
                zmq_msg_close(&msg);
                break;
            }

            if (frame_idx == 0) {
                size_t raw_size = zmq_msg_size(&msg);
                unsigned char *raw_data = (unsigned char *) zmq_msg_data(&msg);

                identity_len = (raw_size < sizeof(identity_buf) - 1) ? raw_size : sizeof(identity_buf) - 1;
                memcpy(identity_buf, raw_data, identity_len);
                identity_buf[identity_len] = '\0';
            }

            if (frame_idx == 1) {
                payload_size = zmq_msg_size(&msg);
                payload = zmq_msg_data(&msg);

                if (payload_size >= EVENT_SIZE_BYTES &&
                    (payload_size % EVENT_SIZE_BYTES) == 0) {

                    size_t num_events = payload_size / EVENT_SIZE_BYTES;

                    uint64_t h = hash_fnv1a((const unsigned char *)identity_buf, identity_len);
                    int worker_idx = (int)(h % N_WORKERS);

                    worker_node_t *wn = &worker_nodes[worker_idx];

                    if (num_events > QUEUE_SIZE) {
                        printf(
                            "ERROR: received batch of %zu events, "
                            "larger than queue capacity %d\n",
                            num_events,
                            QUEUE_SIZE
                        );

                        zmq_msg_close(&msg);
                        continue;
                    }

                    pthread_mutex_lock(&wn->lock);

                    while ((QUEUE_SIZE - wn->count) < num_events && keep_running) {
                        pthread_cond_wait(
                            &wn->space_available,
                            &wn->lock
                        );
                    }

                    if (!keep_running) {
                        pthread_mutex_unlock(&wn->lock);
                        break;
}

                    if (keep_running) {
                        for (size_t e = 0; e < num_events; e++) {
                            queue_item_t ev;
                            ev.item = ITEM_DATA;
                            ev.data.valid = 1;
                            memcpy(ev.data.source_id, identity_buf, identity_len + 1); // +1: include il '\0'

                            size_t off = e * EVENT_SIZE_BYTES;
                            for (size_t w = 0; w < EVENT_SIZE_WORDS; w++) {
                                memcpy(&(ev.data.words[w]),
                                       payload + off + w * 2,
                                       sizeof(uint16_t));
                            }

                            wn->items[wn->tail] = ev;
                            wn->tail = (wn->tail + 1) % QUEUE_SIZE;
                            wn->count++;
                        }

                        if (num_events > 0)
                            pthread_cond_signal(&wn->data_available);
                    }

                    pthread_mutex_unlock(&wn->lock);
                }
            }

            zmq_getsockopt(server_socket, ZMQ_RCVMORE, &more, &more_size);
            zmq_msg_close(&msg);
            frame_idx++;

        } while (more && keep_running);
    }

    printf("receive_data: Thread exiting\n");
    zmq_close(server_socket);
    zmq_ctx_destroy(context);
    return NULL;
}





void *process_data(void *args_void) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    worker_args_t *args = (worker_args_t *)args_void;
    worker_node_t *wn = &worker_nodes[args->worked_idx];

    //FILE *file = args->fout;
    FILE *file = NULL;

    //FILE *file = (FILE *)file_ptr_void;
    long events_processed = 0;
    //char write_buffer[1024 * 128]; // 128KB buffer
    //size_t buffer_used = 0;



    while (keep_running) {
        pthread_mutex_lock(&wn->lock);
        while (wn->count == 0 && keep_running) {
            pthread_cond_wait(&(wn->data_available), &(wn->lock));
        }

        if (!keep_running) {
            pthread_mutex_unlock(&(wn->lock));
            break;
        }

        // Prendi un batch di eventi
        int batch_size = (wn->count > PROCESS_BATCH_SIZE) ? PROCESS_BATCH_SIZE : wn->count;
        queue_item_t batch[batch_size];
        
        for (int i = 0; i < batch_size; i++) {
            batch[i] = wn->items[wn->head];
            wn->head = (wn->head + 1) % QUEUE_SIZE;
            (wn->count)--;
        }

        pthread_cond_broadcast(&(wn->space_available));
        pthread_mutex_unlock(&(wn->lock));

        // Processa il batch
        for (int i = 0; i < batch_size; i++) {
            queue_item_t current_event = batch[i];
            //printf("DEBUG: source identity: '%s'\n", current_event.source_id);
            if (current_event.item == ITEM_DATA){
                if (current_event.data.valid) {
                    uint16_t cut_buffer[6];
                    tank_node *event_node;
                    event_node = get_node(wn->worker_table, current_event.data.source_id);
                    if (event_node == NULL){
                        printf("ERROR: received data for unknown tank '%s' (no OPEN received yet)\n", current_event.data.source_id);
                        continue;
                    }

                    for (int j = 1; j < 7; j++) {
                        cut_buffer[j-1] = current_event.data.words[j];
                    }

                    if (check_crc(cut_buffer) == 0) {
                        uint32_t canale = get_bits(cut_buffer, 3, 5);
                        uint32_t tempo_16_bit = get_bits(cut_buffer, 8, 16);
                        uint32_t coarse_time = (get_bits(cut_buffer, 24, 8) << 20) | 
                                            (get_bits(cut_buffer, 33, 7) << 13) | 
                                            get_bits(cut_buffer, 40, 13);
                        uint32_t tot = get_bits(cut_buffer, 53, 6);
                        uint32_t tdc_trigger_end = get_bits(cut_buffer, 59, 5);
                        uint32_t tdc_time = get_bits(cut_buffer, 69, 5);
                        uint32_t energia = get_bits(cut_buffer, 74, 14);

                        if (coarse_time == 0 && tempo_16_bit == 0 && energia == 0) {
                            continue;
                        }

                        if (event_node->output_format == 0){
                            int written = snprintf(event_node->write_buffer + event_node->buffer_used,
                                            sizeof(event_node->write_buffer) - event_node->buffer_used,
                                            "%u,%u,%u,%u,%u,%u,%u\n",
                                            canale, tempo_16_bit, coarse_time,
                                            tdc_time, tot, tdc_trigger_end, energia);
                            
                            if (written > 0) {
                                event_node->buffer_used += written;
                            }
                            events_processed++;
                        }
                        else if (event_node->output_format == 1){
                            event_record_t rec;
                            rec.canale = canale;
                            rec.tempo_16_bit = tempo_16_bit;
                            rec.coarse_time = coarse_time;
                            rec.tot = tot;
                            rec.tdc_trigger_end = tdc_trigger_end;
                            rec.tdc_time = tdc_time;
                            rec.energia = energia;

                            memcpy(event_node->write_buffer + event_node->buffer_used, &rec, sizeof(rec));
                            int written = sizeof(rec);
                            if (written > 0) {
                                event_node->buffer_used += written;
                            }
                            events_processed++;
                        }

                        
                        if (event_node->buffer_used > sizeof(event_node->write_buffer) - 256) {
                            fwrite(event_node->write_buffer, 1, event_node->buffer_used, event_node->output_file);
                            fflush(event_node->output_file);

                            event_node->bytes_written += event_node->buffer_used;
                            event_node->buffer_used = 0;
                            if(event_node->bytes_written >= MAX_FILE_SIZE_BYTES){
                                rollover_file(event_node);
                            }
                        }

                        if (events_processed % NUM_EVENTS_WRITE == 0){
                            fwrite(event_node->write_buffer, 1, event_node->buffer_used, event_node->output_file);
                            fflush(event_node->output_file);

                            event_node->bytes_written += event_node->buffer_used;
                            event_node->buffer_used = 0;
                            if(event_node->bytes_written >= MAX_FILE_SIZE_BYTES){
                                rollover_file(event_node);
                            }
                            
                        }
                        
                        

                    }
                }
            }
            else{
                if (current_event.item == ITEM_OPEN){
                    file = fopen(current_event.path, "w");

                    if (file == NULL) {
                        printf("ERROR: failed to open file '%s' for tank '%s'\n",
                            current_event.path, current_event.source_id);
                    } else {
                        if (write_header(file, &current_event.metainfo) != 0) {
                            printf("ERROR: failed to write header for tank '%s'\n", current_event.source_id);
                            fclose(file);
                            continue;
                        }
                        if (current_event.output_format == 0){
                            fprintf(file, "Channel,Unix_time_16_bit,Coarse_time,TDC_time,ToT_time,TDC_trigger_end,Energy\n");
                        } 
                        fflush(file);

                        add_node(wn->worker_table, current_event.source_id, file, current_event.metainfo, current_event.output_format, current_event.path);
                        printf("Opened file '%s' for tank '%s'\n",
                            current_event.path, current_event.source_id);
                    }
                }

                else if (current_event.item == ITEM_CLOSE){
                    tank_node *node = get_node(wn->worker_table, (const char *)(current_event.source_id));
                    if (node == NULL) {
                        printf("WARNING: CLOSE received for unknown tank '%s'\n", current_event.source_id);
                        continue;
                    }
                    else if (node->output_file == NULL) {
                        printf("WARNING: CLOSE received for tank '%s', but no file is open\n", current_event.source_id);
                    }
                    else {
                        if (node->buffer_used > 0) {
                            fwrite(node->write_buffer, 1, node->buffer_used, node->output_file);
                            fflush(node->output_file);
                            node->buffer_used = 0;
                        }
                        fclose(node->output_file);
                        node->output_file = NULL;
                        printf("Closed file for tank '%s'\n", current_event.source_id);
                    } 

                }
            }
        }

        
       
    }

    
    for (size_t b = 0; b < wn->worker_table->num_buckets; b++) {
        for (tank_node *node = wn->worker_table->buckets[b]; node != NULL; node = node->next) {
            if (node->output_file != NULL && node->buffer_used > 0) {
                fwrite(node->write_buffer, 1, node->buffer_used, node->output_file);
                fflush(node->output_file);
                node->buffer_used = 0;
            }
        }
    }

   
    return NULL;
}


/*
int run(int duration, const char *output_path, int flag_flush){

    //reset_state();

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    

    FILE *fout = fopen(output_path, flag_flush == 1 ? "a" : "w");
   
    
    
    if (!fout) {
        perror("Error opening output file");
        return 1;
    }

    if (flag_flush == 0){
        fprintf(fout, "Channel,Unix_time_16_bit,Coarse_time,TDC_time,ToT_time,TDC_trigger_end,Energy\n");
    }

    pthread_t receiver, cl_thread;
    pthread_t workers[N_WORKERS];

    pthread_create(&receiver, NULL, receive_data, NULL);
    //pthread_create(&processing, NULL, process_data, fout);
    //pthread_create(&rc_thread, NULL, run_control, NULL);
    pthread_create(&cl_thread, NULL, control_listener, NULL);

    worker_args_t worker_args[N_WORKERS];

    for(int i = 0; i < N_WORKERS; i++){
        reset_worker_node(&worker_nodes[i]);
        worker_args[i].worked_idx = i;
        worker_args[i].fout = fout;
        pthread_create(&workers[i], NULL, process_data, &worker_args[i]);
    }

    time_t start = time(NULL);

    while (keep_running) {
        if (duration > 0){
            time_t now = time(NULL);
            if (difftime(now, start) >= duration){
                printf("Acquisition time (%d sec) elapsed, stop!\n", duration);
                keep_running = 0;
                break;
            }
        }
        sleep(1);
    }

    //printf("DEBUG: Out of while loop, now joining threads\n");

    for(int i = 0; i < N_WORKERS; i++){
        pthread_cond_broadcast(&(worker_nodes[i].data_available));
        pthread_cond_broadcast(&(worker_nodes[i].space_available));
    }
    
    pthread_join(receiver, NULL);
    //printf("DEBUG: receiver thread joined\n");
    //pthread_join(processing, NULL);

    for (int i = 0; i < N_WORKERS; i++){
        pthread_join(workers[i], NULL);
    }
    //pthread_join(rc_thread, NULL);
    pthread_join(cl_thread, NULL);


    fclose(fout);
    //printf("DEBUG: File closed\n");

    if (duration > 0 && time(NULL) - start >= duration)
        return 1;
    else
        return 0;

}

*/

int run(void) {
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    pthread_t receiver, cl_thread;
    pthread_t workers[N_WORKERS];

    worker_args_t worker_args[N_WORKERS];

    for(int i = 0; i < N_WORKERS; i++){
        reset_worker_node(&worker_nodes[i]);
        worker_args[i].worked_idx = i;
        pthread_create(&workers[i], NULL, process_data, &worker_args[i]);
    }

    pthread_create(&receiver, NULL, receive_data, NULL);
    pthread_create(&cl_thread, NULL, control_listener, NULL);

    while(keep_running){
        sleep(1);
    }

    for(int i = 0; i < N_WORKERS; i++){
        pthread_cond_broadcast(&(worker_nodes[i].data_available));
        pthread_cond_broadcast(&(worker_nodes[i].space_available));
    }

    pthread_join(receiver, NULL);

    for (int i = 0; i < N_WORKERS; i++){
        pthread_join(workers[i], NULL);
    }

    pthread_join(cl_thread, NULL);

    return 0;
}

/*
int main(int argc, char *argv[])
{
    if (argc < 1) {
        fprintf(stderr, "Usage: %s [flag_flush]\n", argv[0]);
        return 2;
    }

    // const char *output_path = argv[1];
    // int duration = atoi(argv[2]);

    int flag_flush = 0;
    if (argc >= 1) {
        flag_flush = atoi(argv[1]);
    }

    if (flag_flush != 0 && flag_flush != 1) {
        fprintf(stderr, "Invalid flag_flush: %d. Use 0 or 1.\n", flag_flush);
        return 2;
    }

    return run(flag_flush);
}

*/


int main(int argc, char *argv[])
{
    (void)argc;
    (void)argv;
    return run();
}




