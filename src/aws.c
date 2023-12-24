// SPDX-License-Identifier: BSD-3-Clause

#include <arpa/inet.h>
#include <asm-generic/socket.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libaio.h>
#include <netinet/in.h>
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

#define SIZE(x) (sizeof(x) / sizeof(*(x)))

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
}

/** Prepare the connection buffer to send the 404 header. */
static void connection_prepare_send_404(struct connection *conn)
{
	conn->state = STATE_SENDING_404;
	int rc;

	strcpy(conn->send_buffer, "HTTP/1.1 404 Not Found\r\n\r\n");
	conn->send_len = strlen(conn->send_buffer);

	rc = w_epoll_add_ptr_out(epollfd, conn->eventfd, conn);
	DIE(rc < 0, "w_epoll_update_fd_out");

	io_prep_pwrite(&conn->iocb, conn->sockfd, conn->send_buffer, conn->send_len,
				   0);
	io_set_eventfd(&conn->iocb, conn->eventfd);
	rc = io_submit(ctx, 1, conn->piocb);
	DIE(rc < 0, "io_submit");
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename
	 * should point to the static or dynamic folder.
	 */
	puts(conn->request_path);
	return RESOURCE_TYPE_NONE;
}

/** Initialize connection structure on given socket. */
struct connection *connection_create(int sockfd)
{
	struct connection *conn;

	conn = malloc(sizeof(*conn));
	DIE(!conn, "malloc");

	conn->sockfd = sockfd;
	conn->eventfd = eventfd(0, 0);
	DIE(conn->eventfd < 0, "eventfd");

	conn->state = STATE_INITIAL;
	*conn->piocb = &conn->iocb;
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);
	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
}

/** Remove connection handler. */
void connection_remove(struct connection *conn)
{
	w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
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
	int recv_bytes;
	int rc;

	recv_bytes = recv(conn->sockfd, conn->recv_buffer, BUFSIZ, 0);
	if (recv_bytes < 0) {
		// dlog(LOG_ERR, "Error in communication from %s\n",
		//  inet_ntoa(conn->addr.sin_addr));
		goto close_connection;
	} else if (!recv_bytes) {
		// dlog(LOG_INFO, "Connection closed from %s\n",
		//  inet_ntoa(conn->addr.sin_addr));
		goto close_connection;
	}

	conn->recv_len = recv_bytes;
	conn->state = STATE_REQUEST_RECEIVED;
	return;

close_connection:
	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");

	connection_remove(conn);
}

/** Open file and update connection fields. */
int connection_open_file(struct connection *conn)
{
	return conn->fd = open(conn->request_path, O_RDONLY);
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
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
	return STATE_NO_STATE;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	return -1;
}

int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	return 0;
}

/**
 * Handle input information: may be a new message or notification of
 * completion of an asynchronous I/O operation.
 */
void handle_input(struct connection *conn)
{
	switch (conn->state) {

	/* Received a HTTP request. */
	case STATE_INITIAL:
		receive_data(conn);
		parse_header(conn);
		conn->state = STATE_REQUEST_RECEIVED;
		conn->res_type = connection_get_resource_type(conn);

		if (conn->res_type == RESOURCE_TYPE_NONE)
			/* The file doesn't exist, send a 404. */
			connection_prepare_send_404(conn);
		break;

	default:
		ERR("Unexpected state\n");
		exit(1);
	}
}

/**
 * Handle output information: may be a new valid requests or
 * notification of completion of an asynchronous I/O operation or invalid
 * requests.
 */
void handle_output(struct connection *conn)
{
	switch (conn->state) {

	/* A 404 error was sent. */
	case STATE_SENDING_404:
		connection_remove(conn);
		break;

	default:
		ERR("Unexpected state\n");
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
	}
	if (event & EPOLLOUT) {
		dlog(LOG_INFO, "Ready to send message %s\n", addr);
		handle_output(conn);
	}
}

int main(void)
{
	int rc;

	/* Initialize asynchronous operations. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "epoll_create1");

	/* Initialize multiplexing. */
	rc = io_setup(10, &ctx);
	DIE(rc < 0, "io_setup");

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
				dlog(LOG_INFO, "New connection\n");
				if (rev.events & EPOLLIN) {
					dlog(LOG_INFO, "New connection\n");
					if (rev.events & EPOLLIN) {
						handle_new_connection();
					}

					continue;
					continue;
				}
				continue;
			}
		}

		/* A connection socket got an I/O event. */
		handle_client(rev.events, rev.data.ptr);
	}

	return 0;
}
