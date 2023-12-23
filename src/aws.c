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

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename
	 * should point to the static or dynamic folder.
	 */
	return RESOURCE_TYPE_NONE;
}

/** Initialize connection structure on given socket. */
struct connection *connection_create(int sockfd)
{
	struct connection *conn;

	conn = malloc(sizeof(*conn));
	DIE(!conn, "malloc");

	conn->sockfd = sockfd;
	conn->state = STATE_NO_STATE;
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

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
}

void handle_new_connection(void)
{
	int sockfd;
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(addr);
	struct connection *conn;
	int opt = 1;
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
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */

	return -1;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
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
	return 0;
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

void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */

	switch (conn->state) {
	default:
		printf("shouldn't get here %d\n", conn->state);
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or
	 * notification of completion of an asynchronous I/O operation or invalid
	 * requests.
	 */

	switch (conn->state) {
	default:
		ERR("Unexpected state\n");
		exit(1);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
}

int main(void)
{
	int rc;

	/* Initialize asynchronous operations. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "epoll_create1");

	/* TODO: Initialize multiplexing. */

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
			dlog(LOG_INFO, "New connection\n");
			if (rev.events & EPOLLIN) {
				handle_new_connection();
				continue;
			}
		}

		/* A connection socket got an I/O event. */
		if (rev.events & EPOLLIN) {
			dlog(LOG_INFO, "New message\n");
			handle_input(rev.data.ptr);
		}
		if (rev.events & EPOLLOUT) {
			dlog(LOG_INFO, "Ready to send message\n");
			handle_output(rev.data.ptr);
		}
	}

	return 0;
}
