
#include "gfserver-student.h"
#define BUFSIZE 4000
#define SCHEME "GETFILE"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <netdb.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <getopt.h>
// #include "content.h"
// #include "content.c"

/* 
 * Modify this file to implement the interface specified in
 * gfserver.h.
 */

typedef struct gfserver_t {
    struct sockaddr_in server_addr;
    int max_npending;
    // ssize_t (*handler)(gfcontext_t *, char *, void*);
    gfh_error_t (*handler)(gfcontext_t **, const char *, void*);
    void* arg;
    char* req_path;
} gfserver_t;

typedef struct gfcontext_t {
    int client_socketfd;
} gfcontext_t;

void gfs_abort(gfcontext_t **ctx){
    if (close((*ctx)->client_socketfd) < 0) {
        perror("Could not close socket\n");
        exit(1);
    }

    free(*ctx);
}

gfserver_t* gfserver_create(){
    /* Socket Code Here */
    gfserver_t* gfs = (gfserver_t*)malloc(sizeof(gfserver_t));

    gfs->server_addr.sin_family = AF_INET;
    gfs->server_addr.sin_addr.s_addr = INADDR_ANY;

    return gfs;
}

ssize_t gfs_send(gfcontext_t **ctx, const void *data, size_t len){
    ssize_t write_size = 0, total_write_transferred = 0;
    // printf("gfs_send\n");
    do {
        // printf("write_size %zd \n", write_size);
        // printf("total_write_transferred %zd \n", total_write_transferred);
        if((write_size = write((*ctx)->client_socketfd , data , len)) < 0)
        {
            perror("Send failed");
            return -1;
        }
        data += write_size;
        len -= write_size;
        total_write_transferred += write_size;
    } while (write_size > 0);

    return total_write_transferred;
}

char* status_to_string(gfstatus_t status) {
    char* status_string;

    switch (status) {
        case 200:
            status_string = "OK";
            break;
        case 400:
            status_string = "FILE_NOT_FOUND";
            break;
        case 500:
            status_string = "ERROR";
            break;
    }

    return status_string;
}

ssize_t gfs_sendheader(gfcontext_t **ctx, gfstatus_t status, size_t file_len){
    char* header_buffer;
    char* header_buffer_save;
    ssize_t write_size, total_write_size = 0, header_size;

    header_buffer = (char*)calloc(BUFSIZE, sizeof(char));
    header_buffer_save = header_buffer;
    if (file_len == 0) {
        header_size = sprintf(header_buffer, "%s %s\r\n\r\n", SCHEME, status_to_string(status));
    }
    else {
        header_size = sprintf(header_buffer, "%s %s %zu\r\n\r\n", SCHEME, status_to_string(status), file_len);
    }

    do {
        if((write_size = write((*ctx)->client_socketfd , header_buffer , header_size)) < 0)
        {
            perror("Send header failed");
            exit(1);
        }
        header_buffer += write_size;
        header_size -= write_size;
        total_write_size += write_size;
    } while (write_size > 0);
    free(header_buffer_save);
    return total_write_size;
}

/* Return -1 for file not found, return -2 for error */
int parse_request(int request_socketfd, gfserver_t *gfs) {
    int return_value = 0;
    // printf("parse req \n");
    ssize_t read_size;
    char *read_buffer, *req_buffer, *terminator_received,
            *req_scheme, *req_method, *req_path, *path_test_space, *path_test_slash;

    read_buffer = (char*)calloc(MAX_REQUEST_LEN, sizeof(char));
    req_buffer = (char*)calloc(MAX_REQUEST_LEN, sizeof(char));

    req_scheme = (char*)calloc(10, sizeof(char));
    req_method = (char*)calloc(10, sizeof(char));
    req_path = (char*)calloc(MAX_REQUEST_LEN, sizeof(char));
    // printf("new req_buffer %s \n", req_buffer);
    //Receive request
    read_size = read(request_socketfd, read_buffer, MAX_REQUEST_LEN);
    if (read_size < 0) {
        perror("Read from request socket error\n");
        return_value = -2;
    } else {
        const char* terminator = "\r\n\r\n";
        // const char* terminator = "\r\r=";
        strcat(req_buffer, read_buffer);
        terminator_received = strstr(req_buffer, terminator);
        // printf("Terminator here %s \n", terminator_received);
        if (terminator_received == NULL) {
            printf("Terminator not received\n");
            return_value = -1;
        } else {
            // Parse request
            req_buffer = strtok(read_buffer, terminator);

            // printf("Request: %s\n", req_buffer);

            sscanf(req_buffer, "%s %s %s",
                   req_scheme,
                   req_method,
                   req_path);

            path_test_space = strstr(req_path, " ");
            path_test_slash = strstr(req_path, "/");

            // Validate request
            if (strcmp(req_scheme, SCHEME) != 0) {
                // printf("Invalid scheme received\n");

                return_value = -1;
            } else if (strcmp(req_method, "GET") != 0) {
                // printf("Invalid method received\n");

                return_value = -1;
            }
            else if (path_test_space != NULL || path_test_slash == NULL) {
                //  printf("Invalid path received\n");
                return_value = -1;
            }
            else {
                gfs->req_path = req_path;

                // printf("Request path: %s\n", gfs->req_path);
            }
        }
    }

    free(read_buffer);
    free(req_scheme);
    free(req_method);

    return return_value;
}

ssize_t send_unsuccessful_response(gfcontext_t *ctx, int rc){
    char* header_buffer;
    char* header_buffer_save;
    ssize_t write_size, total_write_size = 0;
    size_t header_size;

    header_buffer = (char*)calloc(BUFSIZE, sizeof(char));
    // printf("hahahah:\n");
    header_buffer_save = header_buffer;
    if (rc == -1) {
        header_size = sprintf(header_buffer, "%s %s\r\n\r\n", SCHEME, status_to_string(400));
    }
    else if (rc == -2) {
        header_size = sprintf(header_buffer, "%s %s\r\n\r\n", SCHEME, status_to_string(500));
    } else {
        printf("Invalid rc\n");
        exit(1);
    }

    do {
        if((write_size = write(ctx->client_socketfd , header_buffer , header_size)) < 0)
        {
            perror("Send header failed\n");
            exit(1);
        }
        header_buffer += write_size;
        header_size -= write_size;
        total_write_size += write_size;
    } while (header_size > 0);
    free(header_buffer_save);
    return total_write_size;
}

int start_server(gfserver_t* gfs) {
    int socketfd, option = 1;

    // Create a socket
    socketfd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketfd == -1) {
        perror("Could not create a socket");
        return -1;
    }

    // Create a socket
    socketfd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketfd == -1) {
        perror("Could not create a socket\n");
        return -1;
    }
    setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));

    // Bind failed
    if( bind(socketfd,(struct sockaddr *)&(gfs->server_addr) , sizeof(gfs->server_addr)) < 0)
    {
        perror("Bind failed\n");
        return -1;
    }

    // Listen
    if (listen(socketfd , gfs->max_npending) < 0){
        perror("Listen failed\n");
        return -1;
    }

    // printf("Listening requests\n");

    return socketfd;
}


void gfserver_serve(gfserver_t **gfs){
    int socketfd, request_socketfd, rc;
    struct sockaddr_in client_addr;

    if ((socketfd = start_server(*gfs)) < 0) {
        exit(1);
    }

    //Accept incoming connection
    ssize_t sockaddr_size = sizeof(struct sockaddr_in);
    while( (request_socketfd = accept(socketfd, (struct sockaddr *)&client_addr, (socklen_t*)&sockaddr_size)) ){
        gfcontext_t* ctx = (gfcontext_t*)malloc(sizeof(gfcontext_t));
        ctx->client_socketfd = request_socketfd;

        if ((rc = parse_request(request_socketfd, *gfs)) < 0) {
            // printf("Unsuccessful response\n");
            send_unsuccessful_response(ctx, rc);
            gfs_abort(&ctx);
            free((*gfs)->req_path);
        } else {
            printf("Handler called\n");
            (*gfs)->handler(&ctx, (*gfs)->req_path, (*gfs)->arg);
            free((*gfs)->req_path);
            free(ctx);
        }
    }
    // printf("out of the while loop \n");
    if (request_socketfd < 0)
    {
        perror("Connection accept failed\n");
        exit(1);
    }
}

void gfserver_set_handlerarg(gfserver_t **gfs, void* arg){
    (*gfs)->arg = arg;
}

void gfserver_set_handler(gfserver_t **gfs, gfh_error_t (*handler)(gfcontext_t **, const char *, void*)){
    (*gfs)->handler = handler;
}

void gfserver_set_maxpending(gfserver_t **gfs, int max_npending){
    (*gfs)->max_npending = max_npending;
}

void gfserver_set_port(gfserver_t **gfs, unsigned short port){
    (*gfs)->server_addr.sin_port = htons(port);
}

/*
 * this routine is used to handle the getfile request
//  */
gfh_error_t gfs_handler(gfcontext_t **ctx, const char *path, void* arg) { 

    // printf("gggggfs handler %s \n", path);
    FILE *f = fopen(path, "rb");
    //   // status file
    if (f == NULL) {
        gfs_sendheader(ctx, 400, 0);
        return -1;
    }
    fseek(f, 0, SEEK_END);
  
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

    char *string = malloc(fsize + 1);
    fread(string, 1, fsize, f);
    fclose(f);

    string[fsize] = 0;
    gfs_sendheader(ctx, 200, fsize);

    // printf("send header %s\n", path);
    gfs_send(ctx, string, fsize);
    free(string);
    // printf("jjjjj send \n");
    return 1;
 
}
