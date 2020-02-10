
#include "gfserver-student.h"

#define USAGE                                                                 \
"usage:\n"                                                                    \
"  gfserver_main [options]\n"                                                 \
"options:\n"                                                                  \
"  -t [nthreads]       Number of threads (Default: 5)\n"                      \
"  -p [listen_port]    Listen port (Default: 20502)\n"                         \
"  -m [content_file]   Content file mapping keys to content files\n"          \
"  -d [delay]          Delay in content_get, default 0, range 0-5000000 (microseconds)\n "	\
"  -h                  Show this help message.\n"                             \

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
  {"port",          required_argument,      NULL,           'p'},
  {"nthreads",      required_argument,      NULL,           't'},
  {"content",       required_argument,      NULL,           'm'},
  {"delay",         required_argument,      NULL,           'd'},
  {"help",          no_argument,            NULL,           'h'},
  {NULL,            0,                      NULL,             0}
};

extern unsigned long int content_delay;

extern gfh_error_t gfs_handler(gfcontext_t **ctx, const char *path, void* arg);

static void _sig_handler(int signo){
  if ((SIGINT == signo) || (SIGTERM == signo)) {
    exit(signo);
  }
}


/* Global variables for thread and task management */
pthread_t *worker_threads;
typedef struct ClientRequest {
    gfcontext_t* ctx;
    char* path;
    void* arg;
} ClientRequest;
steque_t* client_request_queue;
pthread_mutex_t request_queue_mutex; // for client_request_queue
pthread_cond_t request_queue_cv;


/* Queue client request */
void queue_client_request(ClientRequest* client_request) {
    pthread_mutex_lock(&request_queue_mutex);

    steque_enqueue(client_request_queue, (steque_item)client_request);

    printf("[Main] -- enqueue request: %s\n", client_request->path);

    pthread_mutex_unlock(&request_queue_mutex);

    // Signal workers that there is a new task queued
    pthread_cond_broadcast(&request_queue_cv);
}

/* Client request handler */
ssize_t client_request_handler(gfcontext_t *ctx, char *path, void *arg) {
    ClientRequest* client_request = (ClientRequest*)malloc(sizeof(ClientRequest));

    client_request->ctx = ctx;
    client_request->path = path;
    client_request->arg = arg;

    queue_client_request(client_request);

    return 0;
}

/* Work thread task function */
void *assign_worker_to_task(void *arg) {
    long thread_id;

    thread_id = (long)arg;

    printf("[Worker thread #%ld] -- created\n", thread_id);

    for(;;) {
        pthread_mutex_lock(&request_queue_mutex);

        while(steque_size(client_request_queue) == 0) {
            printf("[Worker thread #%ld] -- waiting\n", thread_id);
            pthread_cond_wait(&request_queue_cv, &request_queue_mutex);
        }

        ClientRequest* request = (ClientRequest*)steque_pop(client_request_queue);
        pthread_mutex_unlock(&request_queue_mutex);

        printf("[Worker thread #%ld] -- received request_path: %s\n", thread_id, request->path);

        if (handler_get(request->ctx, request->path, request->arg) < 0) {
            printf("[Worker thread #%ld] -- error during handling request\n", thread_id);
        }

        free(request);
    }
}

/* Main ========================================================= */
int main(int argc, char **argv) {
  int option_char = 0;
  unsigned short port = 20502;
  char *content_map = "content.txt";
  gfserver_t *gfs = NULL;
  int nthreads = 5;

  setbuf(stdout, NULL);

  if (SIG_ERR == signal(SIGINT, _sig_handler)){
    fprintf(stderr,"Can't catch SIGINT...exiting.\n");
    exit(EXIT_FAILURE);
  }

  if (SIG_ERR == signal(SIGTERM, _sig_handler)){
    fprintf(stderr,"Can't catch SIGTERM...exiting.\n");
    exit(EXIT_FAILURE);
  }

  // Parse and set command line arguments
  while ((option_char = getopt_long(argc, argv, "d:t:rhm:p:", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      case 't': // nthreads
        nthreads = atoi(optarg);
        break;
      case 'p': // listen-port
        port = atoi(optarg);
        break;
      case 'h': // help
        fprintf(stdout, "%s", USAGE);
        exit(0);
        break;       
      default:
        fprintf(stderr, "%s", USAGE);
        exit(1);
      case 'm': // file-path
        content_map = optarg;
        break;                                     
      case 'd': // delay
        content_delay = (unsigned long int) atoi(optarg);
        break;     
    }
  }

  /* not useful, but it ensures the initial code builds without warnings */
  if (nthreads < 1) {
    nthreads = 1;
  }

	if (content_delay > 5000000) {
		fprintf(stderr, "Content delay must be less than 5000000 (microseconds)\n");
		exit(__LINE__);
	}

  content_init(content_map);

  /* Initialize thread management */
   // Initialize global variables related to worker and task queue management
    pthread_mutex_init(&request_queue_mutex, NULL);
    pthread_cond_init(&request_queue_cv, NULL);
    client_request_queue = (steque_t*) malloc(sizeof(steque_t));
    steque_init(client_request_queue);

  /*Initializing server*/
  gfs = gfserver_create();

  /*Setting options*/
  gfserver_set_port(&gfs, port);
  gfserver_set_maxpending(&gfs, 16);
  gfserver_set_handler(&gfs, gfs_handler);
  gfserver_set_handlerarg(&gfs, NULL); // doesn't have to be NULL!

  /*Loops forever*/
  gfserver_serve(&gfs);
}
