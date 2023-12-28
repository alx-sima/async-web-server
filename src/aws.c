// SPDX-License-Identifier: BSD-3-Clause

#include <arpa/inet.h>
#include <asm-generic/socket.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libaio.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "aws.h"
#include "http-parser/http_parser.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/util.h"
#include "utils/w_epoll.h"

#define CHUNKS 16
#define CHUNK_SIZE (BUFSIZ / CHUNKS)

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define SIZE(x) (sizeof(x) / sizeof(*(x)))

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

/** Prepare the connection buffer to send the reply header. */
static void connection_prepare_send_reply_header(struct connection *conn)
{
	conn->state = STATE_SENDING_HEADER;

	snprintf(conn->send_buffer, BUFSIZ,
			 "HTTP/1.1 200 OK\r\n"
			 "Connexion: close\r\n"
			 "Content-Length: %ld\r\n"
			 "\r\n",
			 conn->file_size);
	conn->send_len = strlen(conn->send_buffer);
	dlog(LOG_DEBUG, "Prepared reply header (%lu bytes)\n", conn->send_len);
}

/** Prepare the connection buffer to send the 404 header. */
static void connection_prepare_send_404(struct connection *conn)
{
	conn->state = STATE_SENDING_404;
	strcpy(conn->send_buffer, "HTTP/1.1 404 Not Found\r\n\r\n");
	conn->send_len = strlen(conn->send_buffer);
}

/**
 * Get resource type depending on request path/filename. Filename
 * should point to the static or dynamic folder.
 */
static enum resource_type connection_get_resource_type(struct connection *conn)
{
	static const char static_path[] = "/static";
	static const char dynamic_path[] = "/dynamic";
	enum resource_type type = RESOURCE_TYPE_NONE;

	if (!strncmp(conn->request_path, static_path, SIZE(static_path) - 1))
		type = RESOURCE_TYPE_STATIC;
	else if (!strncmp(conn->request_path, dynamic_path, SIZE(dynamic_path) - 1))
		type = RESOURCE_TYPE_DYNAMIC;

	if (connection_open_file(conn) < 0) {
		/* File doesn't exist, send a 404. */
		if (errno == ENOENT)
			return RESOURCE_TYPE_NONE;
		// TODO
	}
	return type;
}

/** Initialize connection structure on given socket. */
struct connection *connection_create(int sockfd)
{
	struct connection *conn;

	conn = malloc(sizeof(*conn));
	DIE(!conn, "malloc");

	conn->sockfd = sockfd;
	conn->eventfd = eventfd(0, EFD_NONBLOCK);
	DIE(conn->eventfd < 0, "eventfd");

	conn->state = STATE_INITIAL;
	conn->async_read_len = 0;
	conn->file_pos = 0;

	memset(conn->send_buffer, 0, BUFSIZ);
	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	conn->state = STATE_ASYNC_ONGOING;
	dlog(LOG_DEBUG, "Starting async IO\n");

	memset(conn->send_buffer, 0, SIZE(conn->send_buffer));
	conn->send_pos = 0;

	int rc = w_epoll_add_ptr_in(epollfd, conn->eventfd, conn);

	io_setup(CHUNKS, &conn->ctx);
	conn->send_len = MIN(BUFSIZ, conn->file_size - conn->file_pos);
	struct iocb chunks[CHUNKS];
	struct iocb *pchunks[CHUNKS];
	int used_chunks =
		conn->send_len / CHUNK_SIZE + (conn->send_len % CHUNK_SIZE != 0);
	int offset = conn->file_pos;

	for (int i = 0; i < used_chunks; ++i) {
		int size = MIN(CHUNK_SIZE, conn->file_size - offset);

		io_prep_pread(&chunks[i], conn->fd, conn->send_buffer, size, offset);
		io_set_eventfd(&chunks[i], conn->eventfd);
		pchunks[i] = &chunks[i];
		offset += CHUNK_SIZE;
	}
	rc = io_submit(conn->ctx, used_chunks, pchunks);
	w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "io_submit");
}

/** Remove connection handler. */
void connection_remove(struct connection *conn)
{
	char addrbuf[64];
	int rc;

	rc = get_peer_address(conn->sockfd, addrbuf, sizeof(addrbuf));
	if (rc < 0)
		dlog(LOG_ERR, "get_peer_address: %s\n", strerror(errno));
	else
		dlog(LOG_INFO, "Closing connection with %s\n", addrbuf);

	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	if (rc < 0)
		dlog(LOG_ERR, "w_epoll_remove_ptr: %s\n", strerror(errno));

	rc = w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	if (rc < 0)
		dlog(LOG_ERR, "w_epoll_remove_ptr: %s\n", strerror(errno));

	rc = close(conn->fd);
	if (rc < 0)
		dlog(LOG_ERR, "close: %s\n", strerror(errno));

	rc = close(conn->sockfd);
	if (rc < 0)
		dlog(LOG_ERR, "close: %s\n", strerror(errno));

	rc = close(conn->eventfd);
	if (rc < 0)
		dlog(LOG_ERR, "close: %s\n", strerror(errno));

	free(conn);
}

void handle_new_connection(void)
{
	struct connection *conn;
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(addr);
	int sockfd;
	int rc;

	/* Handle a new connection request on the server socket. */

	/* Accept new connection. */
	sockfd = accept(listenfd, (SSA *)&addr, &addrlen);
	DIE(sockfd < 0, "accept");

	dlog(LOG_INFO, "Accepted connection from: %s:%d\n",
		 inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

	/* Set socket to be non-blocking. */
	rc = fcntl(sockfd, F_SETFL, O_NONBLOCK);
	DIE(rc < 0, "fcntl");

	/* Instantiate new connection handler. */
	conn = connection_create(sockfd);

	/* Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_ptr_in");

	/* Initialize HTTP_REQUEST parser. */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;
}

/**
 * Receive message on socket.
 * Store message in recv_buffer in struct connection.
 */
void receive_data(struct connection *conn)
{
	int rc;

	rc = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ, 0);
	if (rc < 0) {
		dlog(LOG_ERR, "Error in communication\n");
		connection_remove(conn);
	} else if (rc == 0) {
		dlog(LOG_ERR, "Connection closed by peer\n");
		connection_remove(conn);
	}

	conn->recv_len += rc;
	/* A HTTP request must end in 2 newlines. */
	if (strstr(conn->recv_buffer, "\r\n\r\n") != NULL) {
		parse_header(conn);

		conn->res_type = connection_get_resource_type(conn);
		if (conn->res_type == RESOURCE_TYPE_NONE)
			/* The file doesn't exist, send a 404. */
			connection_prepare_send_404(conn);
		else
			connection_prepare_send_reply_header(conn);

		/* Track the sending of the packages over the socket. */
		w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
	}
}

/** Open file and update connection fields. */
int connection_open_file(struct connection *conn)
{
	char path[BUFSIZ] = AWS_DOCUMENT_ROOT;
	struct stat stat;

	strncat(path, conn->request_path, SIZE(path) - strlen(path) - 1);

	conn->fd = open(path, O_RDONLY | O_NONBLOCK);
	if (conn->fd < 0)
		return conn->fd;

	fstat(conn->fd, &stat);
	conn->file_size = stat.st_size;
	return conn->fd;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	conn->state = STATE_SENDING_DATA;
	conn->send_pos = 0;
	w_epoll_add_ptr_out(epollfd, conn->sockfd, conn);

	dlog(LOG_DEBUG, "Async IO completed\n");
}

/** Parse the HTTP header and extract the file path. */
int parse_header(struct connection *conn)
{
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {.on_message_begin = 0,
											 .on_header_field = 0,
											 .on_header_value = 0,
											 .on_path = aws_on_path_cb,
											 .on_url = 0,
											 .on_fragment = 0,
											 .on_query_string = 0,
											 .on_body = 0,
											 .on_headers_complete = 0,
											 .on_message_complete = 0};
	size_t bytes_parsed =
		http_parser_execute(&conn->request_parser, &settings_on_path,
							conn->recv_buffer, conn->recv_len);

	dlog(LOG_DEBUG, "Parsed HTTP request, bytes: %lu, path: %s\n", bytes_parsed,
		 conn->request_path);
	return bytes_parsed;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	conn->state = STATE_SENDING_DATA;
	int rc;

	rc = sendfile(conn->sockfd, conn->fd, 0, conn->send_len);
	DIE(rc < 0, "sendfile");

	if (rc == 0) {
		dlog(LOG_INFO, "Package sent\n");
		connection_remove(conn);
		return STATE_NO_STATE;
	}

	dlog(LOG_DEBUG, "Sent %d bytes of static data\n", rc);
	return STATE_SENDING_DATA;
}

/**
 * Send as much data as possible from the connection send buffer.
 * Returns the number of bytes sent or -1 if an error occurred
 */
int connection_send_data(struct connection *conn)
{
	int rc;

	rc = send(conn->sockfd, conn->send_buffer + conn->send_pos,
			  conn->send_len - conn->send_pos, O_NONBLOCK);
	if (rc < 0)
		return rc;

	conn->send_pos += rc;
	return rc;
}

/**
 * Send data asynchronously.
 * Returns bytes sent on success and -1 on error.
 */
int connection_send_dynamic(struct connection *conn)
{
	int rc;

	rc = connection_send_data(conn);
	if (rc < 0) {
		ERR("send");
		connection_remove(conn);
		return -1;
	}

	if (rc == 0) {
		if (conn->async_read_len == conn->file_size) {
			dlog(LOG_INFO,
				 "Package sent completely (%ld bytes). Closing connection\n",
				 conn->file_size);
			connection_remove(conn);
			return 0;
		}

		/* The file was larger than the buffer,
		 * start reading another block again.
		 */
		dlog(LOG_INFO, "Sent block of %ld bytes\n", conn->send_len);
		connection_start_async_io(conn);
		return 0;
	}

	dlog(LOG_DEBUG, "Sent round of %d bytes\n", rc);
	conn->file_pos += rc;
	return rc;
}

/**
 * Handle input information: may be a new message or notification of
 * completion of an asynchronous I/O operation.
 */
void handle_input(struct connection *conn)
{
	uint64_t nr;
	int rc = read(conn->eventfd, &nr, sizeof(uint64_t));

	if (rc == sizeof(uint64_t)) {
		struct io_event events[nr];

		io_getevents(conn->ctx, 1, nr, events, NULL);
		for (int i = 0; i < nr; ++i) {
			if (events[i].res < 0) {
				ERR("io_getevents");
				exit(1);
			}

			conn->async_read_len += events[i].res;
			if (conn->async_read_len == conn->file_size ||
				conn->async_read_len == SIZE(conn->send_buffer)) {
				connection_complete_async_io(conn);
			}
		}
		return;
	}

	receive_data(conn);
}

/**
 * Handle output information: may be a new valid requests or
 * notification of completion of an asynchronous I/O operation or invalid
 * requests.
 */
void handle_output(struct connection *conn)
{
	int rc;

	if (conn->state == STATE_SENDING_404 ||
		conn->state == STATE_SENDING_HEADER) {
		rc = connection_send_data(conn);
		if (rc < 0) {
			ERR("send");
			connection_remove(conn);
			return;
		}

		/* The package hasn't been sent completely. */
		if (rc > 0)
			return;
	}

	switch (conn->state) {
		/* A 404 error was sent. */
	case STATE_SENDING_404:
		connection_remove(conn);
		break;

	/* The header was sent. */
	case STATE_SENDING_HEADER:
		if (conn->res_type == RESOURCE_TYPE_STATIC)
			connection_send_static(conn);
		else
			connection_start_async_io(conn);

		break;

	case STATE_SENDING_DATA:
		if (conn->res_type == RESOURCE_TYPE_STATIC)
			connection_send_static(conn);
		else
			connection_send_dynamic(conn);
		break;

	default:
		ERR("Unexpected state");
		exit(1);
	}
}

/**
 * Handle new client. There can be input and output connections.
 * Take care of what happened at the end of a connection.
 */
void handle_client(uint32_t event, struct connection *conn)
{
	char addr[64];

	if (get_peer_address(conn->sockfd, addr, SIZE(addr)) < 0) {
		ERR("get_peer_address");
		connection_remove(conn);
		return;
	}

	if (event & EPOLLIN) {
		dlog(LOG_INFO, "New message from %s\n", addr);
		handle_input(conn);
	} else if (event & EPOLLOUT) {
		handle_output(conn);
	}
}

int main(void)
{
	int rc;

	/* Initialize asynchronous operations. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "epoll_create1");

	/* Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	/* Add server socket to epoll object */
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	dlog(LOG_INFO, "Server waiting for connections on port %d\n",
		 AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "epoll_wait");

		/* Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */

		/* The server listener got an I/O event. */
		if (rev.data.fd == listenfd) {
			if (rev.events & EPOLLIN) {
				dlog(LOG_INFO, "New connection\n");
				handle_new_connection();
			}
			continue;
		}

		/* A connection socket got an I/O event. */
		handle_client(rev.events, rev.data.ptr);
	}
	return 0;
}
