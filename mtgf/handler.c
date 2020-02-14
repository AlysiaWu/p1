#include "gfserver-student.h"
#include "gfserver.h"
#include "content.h"

#include "workload.h"
#define BUFFER_SIZE 4000
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

ssize_t handler_get(gfcontext_t **ctx, char *path, void* arg){
	int fildes;
	size_t file_len, bytes_transferred;
	ssize_t read_len, write_len;
	char buffer[BUFFER_SIZE];

	if( 0 > (fildes = content_get(path)))
		return gfs_sendheader(ctx, GF_FILE_NOT_FOUND, 0);

	/* Determine the file size */
	file_len = lseek(fildes, 0, SEEK_END);

	gfs_sendheader(ctx, GF_OK, file_len);

	/* Send the file in chunks */
	bytes_transferred = 0;
	while(bytes_transferred < file_len){
		read_len = pread(fildes, buffer, BUFFER_SIZE, bytes_transferred);
		if (read_len <= 0){
			fprintf(stderr, "handle_with_file read error, %zd, %zu, %zu\n", read_len, bytes_transferred, file_len );
			gfs_abort(ctx);
			return -1;
		}
		write_len = gfs_send(ctx, buffer, read_len);
		if (write_len != read_len){
			fprintf(stderr, "handle_with_file write error, %zd != %zd\n", write_len, read_len);
			gfs_abort(ctx);
			return -1;
		}
		bytes_transferred += write_len;
	}

	return bytes_transferred;
}

//
//  The purpose of this function is to handle a get request
//
//  The ctx is a pointer to the "context" operation and it contains connection state
//  The path is the path being retrieved
//  The arg allows the registration of context that is passed into this routine.
//  Note: you don't need to use arg. The test code uses it in some cases, but
//        not in others.
//
gfh_error_t gfs_handler(gfcontext_t **ctx, const char *path, void* arg){
	return gfh_failure;
}

