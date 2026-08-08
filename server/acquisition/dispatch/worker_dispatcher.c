#include "worker_dispatcher.h"


uint64_t hash_fnv1a(const unsigned char *data, size_t len_data)
{
    uint64_t hash = UINT64_C(0xcbf29ce484222325);
    const uint64_t prime = UINT64_C(0x100000001b3);

    for (size_t i = 0; i < len_data; ++i) {
        hash ^= (uint64_t)data[i];
        hash *= prime;
    }

    return hash;
}


tank_table *create_table(size_t num_buckets){
    tank_table *new_table;
    tank_node **array;

    if (num_buckets < 1){
        return (NULL);
    }

    new_table = malloc(sizeof(tank_table));
    if (!new_table) return (NULL);

    array = calloc(num_buckets, sizeof(tank_node *));
    if (!array){
        free(new_table);
        return (NULL);
    } 

    new_table->num_buckets = num_buckets;
    new_table->buckets = array;

    return (new_table);

}

tank_node *create_node(const char *source_id, FILE *output_file, meta_data_t metadata, int output_format, char *file_path){
    tank_node *new_node;
    new_node = malloc(sizeof(tank_node));
    if(!new_node){
        return NULL;
    }

    new_node->source_id = strdup(source_id);
    new_node->output_file = output_file;
    new_node->events_written = 0;
    new_node->buffer_used = 0;
    new_node->metadata = metadata;
    new_node->output_format = output_format;
    new_node->chunk_index = 0;
    new_node->bytes_written = 0;
    strncpy(new_node->file_path, file_path, sizeof(new_node->file_path) - 1);
    new_node->file_path[sizeof(new_node->file_path) - 1] = '\0';
    strncpy(new_node->base_file_path, file_path, sizeof(new_node->base_file_path) - 1);
    new_node->base_file_path[sizeof(new_node->base_file_path) - 1] = '\0';
    new_node->next = NULL;

    return new_node;
}


size_t key_index(const unsigned char *key, size_t key_len, size_t num_buckets){
    size_t index;

    index =((size_t) hash_fnv1a(key, key_len)) % num_buckets;
    return index;
}


int add_node(tank_table *table, const char *source_id, FILE *output_file, meta_data_t metadata, int output_format, char *file_path){

    if (!table){
        return 0;
    }

    if (!source_id || source_id[0] == '\0'){
        return 0;
    }

    if (!output_file){
        return 0;
    }

    if (!file_path || file_path[0] == '\0'){
        return 0;
    }

    size_t index;
    tank_node **buckets_array;
    tank_node *temp; //dove salvare temporaneamente la lista linkata corrispondente a un determinato bucket (cassetto)
    tank_node *node;

    index = key_index((const unsigned char *)source_id, strlen(source_id), table->num_buckets);
    buckets_array = table->buckets;

    temp = buckets_array[index];

    while (temp != NULL)
    {
        if(strcmp(temp->source_id, source_id) == 0){
            if (temp->output_file != NULL && temp->output_file != output_file) {
                if (temp->buffer_used > 0) {
                    fwrite(temp->write_buffer, 1, temp->buffer_used, temp->output_file);
                    fflush(temp->output_file);
                }
                fclose(temp->output_file);
            }
            temp->output_file = output_file;
            strncpy(temp->file_path, file_path, sizeof(temp->file_path) - 1);
            temp->file_path[sizeof(temp->file_path) - 1] = '\0';
            strncpy(temp->base_file_path, file_path, sizeof(temp->base_file_path) - 1);
            temp->base_file_path[sizeof(temp->base_file_path) - 1] = '\0';
            temp->buffer_used = 0;
            temp->metadata = metadata;
            temp->output_format = output_format;
            temp->chunk_index = 0;             
            temp->bytes_written = 0;
            return 1;
        }

        temp = temp -> next;
    }

    node = create_node(source_id, output_file, metadata, output_format, file_path);
    if(!node) return 0;

    if (buckets_array[index]) {
        node->next = buckets_array[index];
    }
    buckets_array[index] = node;
    return 1;
    



}


tank_node *get_node(const tank_table *table, const char *source_id){

    if (!table){
        return NULL;
    }

    if (!source_id || source_id[0] == '\0'){

        return NULL;
    }

    size_t index;
    tank_node **array;
    tank_node *temp;
    index = key_index((const unsigned char*) source_id, strlen(source_id), table->num_buckets);
    array = table->buckets;

    temp = array[index];
    
    while (temp)
    {
        if (strcmp(source_id, temp->source_id) == 0){
            return temp;
        }

        temp = temp->next;
    }

    return NULL;
    

}

void delete_table(tank_table *table){

    if (!table){
        return;
    }
    
    size_t index = 0;
    tank_node *temp, *temp_next;

    while(index < table->num_buckets){
        temp = table->buckets[index];
        while(temp){
            temp_next = temp->next;
            free(temp->source_id);
            free(temp);
            temp = temp_next;
        }

        index++;
    }

    free(table->buckets);
    free(table);
}






