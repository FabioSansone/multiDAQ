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



#define EVENT_SIZE_WORDS 8  //8 parole da 16 bit => dimensione della parola dal DMA
#define EVENT_SIZE_BYTES 16 //16 byte per evento
#define QUEUE_SIZE 4096 //Numero di eventi che mi aspetto da evreceiver
#define PROCESS_BATCH_SIZE 500 //Quanti eventi prelevare dalla coda in un sol colpa per analizzarli e scriverli

#define NUM_EVENTS_WRITE 100 //How much events are periodically written
 
static volatile sig_atomic_t keep_running = 1;


//definisco il singolo evento (array di 8 parole da 16 bit)
typedef struct {
    uint16_t words[EVENT_SIZE_WORDS];
    int valid;
} event_t;

event_t event_queue[QUEUE_SIZE]; //array di 2048 strutture di tipo event_t per ospitare tutte le parole

int queue_head = 0;
int queue_tail = 0;
int queue_count = 0;



pthread_mutex_t lock;
pthread_cond_t data_available  = PTHREAD_COND_INITIALIZER;
pthread_cond_t space_available = PTHREAD_COND_INITIALIZER;


void reset_state() {
    keep_running = 1;
    queue_head = 0;
    queue_tail = 0;
    queue_count = 0;
    
    static int initialized = 0;
    if (!initialized) {
        pthread_mutex_init(&lock, NULL);
        pthread_cond_init(&data_available, NULL);
        pthread_cond_init(&space_available, NULL);
        initialized = 1;
    }
}


static void sig_handler(int _)
{
    (void)_;
    keep_running = 0;
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

        do {
            zmq_msg_t msg;
            zmq_msg_init(&msg);

            if (zmq_msg_recv(&msg, server_socket, 0) == -1) {
                zmq_msg_close(&msg);
                break;
            }

            if (frame_idx == 1) {
                payload_size = zmq_msg_size(&msg);
                payload = zmq_msg_data(&msg);

                
                if (payload_size >= EVENT_SIZE_BYTES &&
                    (payload_size % EVENT_SIZE_BYTES) == 0) {

                    size_t num_events = payload_size / EVENT_SIZE_BYTES;

                    pthread_mutex_lock(&lock);

                    for (size_t e = 0; e < num_events; e++) {
                        while (queue_count >= QUEUE_SIZE && keep_running)
                            pthread_cond_wait(&space_available, &lock);

                        if (!keep_running)
                            break;

                        event_t ev;
                        ev.valid = 1;

                        size_t off = e * EVENT_SIZE_BYTES;
                        for (size_t w = 0; w < EVENT_SIZE_WORDS; w++) {
                            memcpy(&ev.words[w],
                                   payload + off + w * 2,
                                   sizeof(uint16_t));
                        }

                        event_queue[queue_tail] = ev;
                        queue_tail = (queue_tail + 1) % QUEUE_SIZE;
                        queue_count++;
                    }

                    if (num_events > 0)
                        pthread_cond_signal(&data_available);

                    pthread_mutex_unlock(&lock);
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





void *process_data(void *file_ptr_void) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    FILE *file = (FILE *)file_ptr_void;
    long events_processed = 0;
    char write_buffer[1024 * 128]; // 128KB buffer
    size_t buffer_used = 0;



    while (keep_running) {
        pthread_mutex_lock(&lock);
        while (queue_count == 0 && keep_running) {
            pthread_cond_wait(&data_available, &lock);
        }

        if (!keep_running) {
            pthread_mutex_unlock(&lock);
            break;
        }

        // Prendi un batch di eventi
        int batch_size = (queue_count > PROCESS_BATCH_SIZE) ? PROCESS_BATCH_SIZE : queue_count;
        event_t batch[batch_size];
        
        for (int i = 0; i < batch_size; i++) {
            batch[i] = event_queue[queue_head];
            queue_head = (queue_head + 1) % QUEUE_SIZE;
            queue_count--;
        }

        pthread_cond_broadcast(&space_available);
        pthread_mutex_unlock(&lock);

        // Processa il batch
        for (int i = 0; i < batch_size; i++) {
            event_t current_event = batch[i];
            if (current_event.valid) {
                uint16_t cut_buffer[6];
                for (int j = 1; j < 7; j++) {
                    cut_buffer[j-1] = current_event.words[j];
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

                    if (coarse_time == 0) {
                        continue;
                    }

                    int written = snprintf(write_buffer + buffer_used, 
                                         sizeof(write_buffer) - buffer_used,
                                         "%u,%u,%u,%u,%u,%u,%u\n",
                                         canale, tempo_16_bit, coarse_time, 
                                         tdc_time, tot, tdc_trigger_end, energia);
                    if (written > 0) {
                        buffer_used += written;
                    }
                    events_processed++;

                    
                    if (buffer_used > sizeof(write_buffer) - 256) {
                        pthread_mutex_lock(&lock);
                        fwrite(write_buffer, 1, buffer_used, file);
                        fflush(file);
                        pthread_mutex_unlock(&lock);
                        buffer_used = 0;
                    }

                    if (events_processed % NUM_EVENTS_WRITE == 0){
                        pthread_mutex_lock(&lock);
                        fwrite(write_buffer, 1, buffer_used, file);
                        fflush(file);
                        pthread_mutex_unlock(&lock);
                        buffer_used = 0;
                    }
                }
            }
        }

        
       
    }

    
    if (buffer_used > 0) {
        pthread_mutex_lock(&lock);
        fwrite(write_buffer, 1, buffer_used, file);
        fflush(file);
        pthread_mutex_unlock(&lock);
    }

   
    return NULL;
}



int run(int duration, const char *output_path, int flag_flush){

    reset_state();

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

    pthread_t receiver, processing, rc_thread;

    pthread_create(&receiver, NULL, receive_data, NULL);
    pthread_create(&processing, NULL, process_data, fout);
    pthread_create(&rc_thread, NULL, run_control, NULL);

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

    pthread_cond_broadcast(&data_available);
    pthread_cond_broadcast(&space_available);
    pthread_join(receiver, NULL);
    //printf("DEBUG: receiver thread joined\n");
    pthread_join(processing, NULL);
    pthread_join(rc_thread, NULL);


    fclose(fout);
    //printf("DEBUG: File closed\n");

    if (duration > 0 && time(NULL) - start >= duration)
        return 1;
    else
        return 0;

}



int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <output_path> <duration_s> [flag_flush]\n", argv[0]);
        return 2;
    }

    const char *output_path = argv[1];
    int duration = atoi(argv[2]);

    int flag_flush = 0;
    if (argc >= 4) {
        flag_flush = atoi(argv[3]);
    }

    if (flag_flush != 0 && flag_flush != 1) {
        fprintf(stderr, "Invalid flag_flush: %d. Use 0 or 1.\n", flag_flush);
        return 2;
    }

    return run(duration, output_path, flag_flush);
}





