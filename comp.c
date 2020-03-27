#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include "compress.h"
#include "chunk_archive.h"
#include "queue.h"
#include "options.h"

#define CHUNK_SIZE (1024*1024)
#define QUEUE_SIZE 20

#define COMPRESS 1
#define DECOMPRESS 0

struct thread_info{
	pthread_t thread_id;
	int thread_num;	
};

struct args{
	queue in;
	queue out;
	chunk (*process)(chunk);
        pthread_mutex_t *mutex;
	int * nchunks;
};

struct argsio{
	queue in;
 	queue out;
	archive ar;
	int fd;
	int nchunks;
        struct options opt;
};

// take chunks from queue in, run them through process (compress or decompress), send them to queue out
void * worker(void * arg) {
    struct args * args  = arg;
    chunk ch, res;
    while(1){
	pthread_mutex_lock(args->mutex);
	if ( ((*args->nchunks)--)<=0){
		pthread_mutex_unlock(args->mutex);
		break;
	}
	pthread_mutex_unlock(args->mutex);
        ch = q_remove(args->in);
        res = args->process(ch);
        free_chunk(ch);
        q_insert(args->out, res);
    }
}

//Lectura para compresion
void *readComp(void * arg){
	struct argsio * args = arg;
        chunk ch;
        int offset;
	for(int i=0; i<args->nchunks; i++) {
        	ch = alloc_chunk(args->opt.size);
        	offset=lseek(args->fd, 0, SEEK_CUR);
        	ch->size   = read(args->fd, ch->data, args->opt.size);
       		ch->num    = i;
        	ch->offset = offset;
        	q_insert(args->in, ch);
    	}
}

//Escritura para compresion
void *writeComp(void * arg){
    struct argsio * args = arg;
    chunk ch;
    for(int i=0; i<args->nchunks; i++) {
        ch = q_remove(args->out);
        add_chunk(args->ar, ch);
        free_chunk(ch);
    }

}

//Lectura para decompresion
void *readDecomp(void * arg){
	struct argsio * args = arg;
        chunk ch;
	for(int i=0; i<chunks(args->ar); i++) {
        	ch = get_chunk(args->ar, i);
        	q_insert(args->in, ch);
    }        
}

//Escritura para decompresion
void *writeDecomp(void * arg){
    struct argsio * args = arg;
    chunk ch;
    for(int i=0; i<chunks(args->ar); i++) {
        ch=q_remove(args->out);
        lseek(args->fd, ch->offset, SEEK_SET);
        write(args->fd, ch->data, ch->size);
        free_chunk(ch);
    }
   
}

// Compress file taking chunks of opt.size from the input file,
// inserting them into the in queue, running them using a worker,
// and sending the output from the out queue into the archive file
void comp(struct options opt) {
    int fd, chunks, i, offset;
    struct stat st;
    char comp_file[256];
    archive ar;
    queue in, out;
    //declaramos las variables
    struct thread_info *threads;
    struct args *args;
    struct argsio *argsio;
    pthread_t escritor;
    pthread_t lector;
    pthread_mutex_t *mutex; //no se eu
    if((fd=open(opt.file, O_RDONLY))==-1) {
        printf("Cannot open %s\n", opt.file);
        exit(0);
    }

    fstat(fd, &st);
    chunks = st.st_size/opt.size+(st.st_size % opt.size ? 1:0);

    if(opt.out_file) {
        strncpy(comp_file,opt.out_file,255);
    } else {
        strncpy(comp_file, opt.file, 255);
        strncat(comp_file, ".ch", 255);
    }

    ar = create_archive_file(comp_file);

    in  = q_create(opt.queue_size);
    out = q_create(opt.queue_size);

    // read input file and send chunks to the in queue
    argsio = malloc(sizeof(struct argsio));
    argsio-> in = in;
    argsio->out = out;
    argsio->ar = ar;
    argsio->fd = fd;
    argsio->nchunks = chunks;
    argsio->opt = opt;
    //Lanzamos los threads
    if (pthread_create(&lector,NULL,readComp,argsio) != 0){
	printf("No se pudo crear el thread");
	exit(1);
     };


    // compression of chunks from in to out
    threads = malloc(sizeof(struct thread_info) * opt.num_threads);
    args = malloc(sizeof(struct args));
    if (threads == NULL || args==NULL) {
	printf("Not enough memory\n");
	exit(1);
    }
    if ((mutex= malloc(sizeof(pthread_mutex_t)))==NULL){
	printf("Out of memory\n");
	exit(1);
    }
    //inicializamos struct args
    int numberchunks = chunks;
    args->in = in;
    args->out = out;
    args->process = zcompress;
    args->mutex = mutex;
    pthread_mutex_init(args->mutex,NULL);
    args->nchunks = &numberchunks;
    //inizializar threads con for 
    for (i = 0; i < opt.num_threads; i++) {
        threads[i].thread_num = i;
	if (pthread_create(&threads[i].thread_id,NULL,worker,args) != 0){
		printf("No se pudo crear el thread: %d",i);
		exit(1);
	};
    }

    // send chunks to the output archive file
    if (pthread_create(&escritor,NULL,writeComp,argsio) != 0){
	printf("No se pudo crear el thread");
	exit(1);
     };
    //Wait for the threads to finish
    for (i = 0; i< opt.num_threads; i++){
    	pthread_join(threads[i].thread_id, NULL);
    }
    pthread_join(lector,NULL);
    pthread_join(escritor,NULL);

    //destroy mutex
    pthread_mutex_destroy(args->mutex);
    //frees
    free(threads);
    free(args);
    free(args->mutex);
    free(argsio);   

    close_archive_file(ar);
    close(fd);
    q_destroy(in);
    q_destroy(out);
}


// Decompress file taking chunks of opt.size from the input file,
// inserting them into the in queue, running them using a worker,
// and sending the output from the out queue into the decompressed file

void decomp(struct options opt) {
    int fd, i;
    struct stat st;
    char uncomp_file[256];
    archive ar;
    queue in, out;
    //Vars
    struct thread_info *threads;
    struct argsio * argsio;
    pthread_mutex_t *mutex; //no se eu
    pthread_t escritor;
    pthread_t lector;

    if((ar=open_archive_file(opt.file))==NULL) {
        printf("Cannot open archive file\n");
        exit(0);
    };

    if(opt.out_file) {
        strncpy(uncomp_file, opt.out_file, 255);
    } else {
        strncpy(uncomp_file, opt.file, strlen(opt.file) -3);
        uncomp_file[strlen(opt.file)-3] = '\0';
    }

    if((fd=open(uncomp_file, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH))== -1) {
        printf("Cannot create %s: %s\n", uncomp_file, strerror(errno));
        exit(0);
    }

    in  = q_create(opt.queue_size);
    out = q_create(opt.queue_size);
    // read chunks with compressed data
    argsio = malloc(sizeof(struct argsio));
    argsio-> in = in;
    argsio->out = out;
    argsio->ar = ar;
    argsio->fd = fd;
    //Lanzamos los threads
    if (pthread_create(&lector,NULL,readDecomp,argsio) != 0){
	printf("No se pudo crear el thread");
	exit(1);
     };


    //inicializamos la estructura
    threads = malloc(sizeof(struct thread_info) * opt.num_threads);
    struct args * args = malloc(sizeof(struct args));
    if ((mutex= malloc(sizeof(pthread_mutex_t)))==NULL){
	printf("Out of memory\n");
	exit(1);
    }
    int numberchunks = chunks(ar);
    args->in = in;
    args->out = out;
    args->process = zdecompress;
    args->mutex = mutex;
    pthread_mutex_init(args->mutex,NULL);
    args->nchunks = &numberchunks;
    //inizializar threads con for 
    for (i = 0; i < opt.num_threads; i++) {
        threads[i].thread_num = i;
	if (pthread_create(&threads[i].thread_id,NULL,worker,args) != 0){
		printf("No se pudo crear el thread: %d",i);
		exit(1);
	};
    }
    // write chunks from output to decompressed file
    if (pthread_create(&escritor,NULL,writeDecomp,argsio) != 0){
	printf("No se pudo crear el thread");
	exit(1);
     };
     //Wait for the threads to finish
     for (i = 0; i< opt.num_threads; i++){
    	pthread_join(threads[i].thread_id, NULL);
     }
     pthread_join(lector,NULL);
     pthread_join(escritor,NULL);
     //destroy mutex
     pthread_mutex_destroy(args->mutex);

    //frees
    free(threads);
    free(args);
    free(mutex);
    free(argsio);   
 
    close_archive_file(ar);
    close(fd);
    q_destroy(in);
    q_destroy(out);
}

int main(int argc, char *argv[]) {
    struct options opt;

    opt.compress    = COMPRESS;
    opt.num_threads = 3;
    opt.size        = CHUNK_SIZE;
    opt.queue_size  = QUEUE_SIZE;
    opt.out_file    = NULL;

    read_options(argc, argv, &opt);

    if(opt.compress == COMPRESS) comp(opt);
    else decomp(opt);
}
