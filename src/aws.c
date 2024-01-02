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

#define MAXEVENTS 8

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define SIZE(x) (sizeof(x) / sizeof(*(x)))

#define LOG_ERRNO(funcname) dlog(LOG_ERR, "%s: %s\n", funcname, strerror(errno))

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
	char buffer[BUFSIZ];

	conn->state = STATE_SENDING_HEADER;
	conn->send_len = snprintf(buffer, SIZE(buffer),
							  "HTTP/1.1 200 OK\r\n"
							  "Connexion: close\r\n"
							  "Content-Length: %ld\r\n"
							  "\r\n",
							  conn->file_size);

	conn->send_buffer = strdup(buffer);
	DIE(conn->send_buffer == NULL, "strdup");
}

/** Prepare the connection buffer to send the 404 header. */
static void connection_prepare_send_404(struct connection *conn)
{
	conn->state = STATE_SENDING_404;
	conn->send_buffer = strdup("HTTP/1.1 404 Not Found\r\n\r\n");
	DIE(conn->send_buffer == NULL, "strdup");
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

	if (strncmp(conn->request_path, static_path, SIZE(static_path) - 1) == 0)
		type = RESOURCE_TYPE_STATIC;
	else if (strncmp(conn->request_path, dynamic_path,
					 SIZE(dynamic_path) - 1) == 0)
		type = RESOURCE_TYPE_DYNAMIC;

	if (connection_open_file(conn) < 0) {
		/* File cannot be accessed, send a 404. */
		return RESOURCE_TYPE_NONE;
	}
	return type;
}

/** Initialize connection structure on given socket. */
struct connection *connection_create(int sockfd)
{
	struct connection *conn;

	conn = malloc(sizeof(*conn));
	DIE(conn == NULL, "malloc");

	conn->fd = -1;
	conn->sockfd = sockfd;
	conn->eventfd = eventfd(0, EFD_NONBLOCK);
	DIE(conn->eventfd < 0, "eventfd");

	conn->state = STATE_INITIAL;
	conn->async_read_len = 0;
	conn->recv_len = 0;
	conn->ctx = 0;

	memset(conn->recv_buffer, 0, SIZE(conn->recv_buffer));
	conn->send_buffer = NULL;
	return conn;
}

/** Remove connection handler. */
void connection_remove(struct connection *conn)
{
	char addrbuf[64];
	int rc;

	rc = get_peer_address(conn->sockfd, addrbuf, sizeof(addrbuf));
	if (rc < 0)
		LOG_ERRNO("get_peer_address");
	else
		dlog(LOG_INFO, "Closing connection with %s\n", addrbuf);

	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	if (rc < 0 && errno != ENOENT)
		LOG_ERRNO("w_epoll_remove_ptr");
	rc = w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	if (rc < 0 && errno != ENOENT)
		LOG_ERRNO("w_epoll_remove_ptr");

	if (conn->fd != -1) {
		rc = close(conn->fd);
		if (rc < 0)
			LOG_ERRNO("close");
	}
	rc = close(conn->eventfd);
	if (rc < 0 && errno != ENOENT)
		LOG_ERRNO("close");
	rc = close(conn->sockfd);
	if (rc < 0 && errno != ENOENT)
		LOG_ERRNO("close");

	if (conn->send_buffer)
		free(conn->send_buffer);
	free(conn);
}

/** Handle a new connection request on the server socket. */
void handle_new_connection(void)
{
	struct connection *conn;
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(addr);
	char addrbuf[64];
	int sockfd;
	int rc;

	/* Accept new connection. */
	sockfd = accept(listenfd, (SSA *)&addr, &addrlen);
	if (sockfd < 0) {
		LOG_ERRNO("accept");
		return;
	}

	rc = get_peer_address(sockfd, addrbuf, sizeof(addrbuf));
	if (rc < 0) {
		LOG_ERRNO("get_peer_address");
		return;
	}

	dlog(LOG_INFO, "Accepted connection from: %s\n", addrbuf);

	/* Get previous fd attributes in order to update them. */
	rc = fcntl(sockfd, F_GETFL, 0);
	if (rc < 0)
		goto fcntl_error;

	/* Set socket to be non-blocking. */
	rc = fcntl(sockfd, F_SETFL, rc | O_NONBLOCK);
	if (rc < 0)
		goto fcntl_error;

	/* Instantiate new connection handler. */
	conn = connection_create(sockfd);

	/* Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	if (rc < 0) {
		LOG_ERRNO("w_epoll_add_ptr_in");
		connection_remove(conn);
		return;
	}

	/* Initialize HTTP_REQUEST parser. */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;
	return;

fcntl_error:
	LOG_ERRNO("fcntl");
	rc = close(sockfd);
	if (rc < 0)
		LOG_ERRNO("close");
}

/**
 * Receive message on socket.
 * Store message in `recv_buffer` in struct connection.
 */
void receive_data(struct connection *conn)
{
	int rc;

	rc = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ, 0);
	if (rc <= 0) {
		if (rc == 0)
			dlog(LOG_ERR, "Connection closed by peer\n");
		else
			dlog(LOG_ERR, "Error in communication\n");
		connection_remove(conn);
		return;
	}

	dlog(LOG_DEBUG, "Received %d bytes\n", rc);
	conn->recv_len += rc;

	/* A HTTP request must end in 2 newlines. */
	if (strstr(conn->recv_buffer, "\r\n\r\n")) {
		parse_header(conn);

		conn->res_type = connection_get_resource_type(conn);
		if (conn->res_type == RESOURCE_TYPE_NONE)
			/* The file doesn't exist, send a 404. */
			connection_prepare_send_404(conn);
		else
			connection_prepare_send_reply_header(conn);

		dlog(LOG_DEBUG, "Prepared reply header (%lu bytes)\n", conn->send_len);

		/* Track the sending of the packages over the socket. */
		w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
		conn->send_pos = 0;
	}
}

/** Open file and update connection fields. */
int connection_open_file(struct connection *conn)
{
	char path[BUFSIZ] = AWS_DOCUMENT_ROOT;
	struct stat stat;
	int rc;

	strncat(path, conn->request_path, SIZE(path) - strlen(path) - 1);

	conn->fd = open(path, O_RDONLY | O_NONBLOCK);
	if (conn->fd < 0)
		return conn->fd;

	rc = fstat(conn->fd, &stat);
	if (rc < 0)
		return rc;

	conn->file_size = stat.st_size;
	return conn->fd;
}

/**
 * Send as much data as possible from the connection send buffer.
 * Returns the number of bytes sent or -1 if an error occurred
 */
int connection_send_data(struct connection *conn)
{
	int rc;

	rc = send(conn->sockfd, conn->send_buffer + conn->send_pos,
			  conn->send_len - conn->send_pos, 0);
	if (rc < 0)
		return rc;

	conn->send_pos += rc;
	return rc;
}

/** Send static data using `sendfile(2)`. */
enum connection_state connection_send_static(struct connection *conn)
{
	conn->state = STATE_SENDING_DATA;
	int rc;

	rc = sendfile(conn->sockfd, conn->fd, 0, conn->file_size);
	if (rc <= 0) {
		if (rc == 0)
			dlog(LOG_INFO, "Package sent\n");
		else
			dlog(LOG_ERR, "Error sending package. Closing connection\n");
		connection_remove(conn);
		return STATE_NO_STATE;
	}

	dlog(LOG_DEBUG, "Sent %d bytes of static data\n", rc);
	conn->file_size -= rc;
	return STATE_SENDING_DATA;
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
		dlog(LOG_ERR, "Error sending package. Closing connection\n");
		connection_remove(conn);
		return -1;
	}

	if (rc == 0) {
		dlog(LOG_INFO,
			 "Package sent completely (%ld bytes). Closing connection\n",
			 conn->file_size);
		connection_remove(conn);
		return 0;
	}

	dlog(LOG_DEBUG, "Sent round of %d bytes\n", rc);
	return rc;
}

/**
 * Launch an asynchronous operation for reading data from the file.
 * Return the number of launched operations, or -1 if there was an error
 * encountered.
 */
int launch_buffer_worker(struct connection *conn)
{
	int rc;

	rc = io_setup(1, &conn->ctx);
	if (rc < 0)
		return rc;

	*conn->piocb = &conn->iocb;
	conn->send_len = conn->file_size;
	conn->send_buffer = realloc(conn->send_buffer, conn->send_len);
	DIE(conn->send_buffer == NULL, "realloc");

	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, conn->send_len, 0);
	io_set_eventfd(&conn->iocb, conn->eventfd);

	return io_submit(conn->ctx, 1, conn->piocb);
}

/**
 * Start asynchronous operation (read from file), using `io_submit(2)` & friends
 * for reading data asynchronously.
 */
void connection_start_async_io(struct connection *conn)
{
	int rc;

	conn->state = STATE_ASYNC_ONGOING;
	dlog(LOG_DEBUG, "Starting async IO\n");

	/* Monitor eventfd to be notified when file reading ended. */
	rc = w_epoll_add_ptr_in(epollfd, conn->eventfd, conn);
	if (rc < 0)
		goto close_connection;
	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	if (rc < 0)
		goto close_connection;

	rc = launch_buffer_worker(conn);
	if (rc > 0)
		return;

close_connection:
	dlog(LOG_ERR, "Error starting async IO. Closing connection\n");
	connection_remove(conn);
}

/**
 * Complete asynchronous operation; operation returns successfully.
 * Prepare socket for sending.
 */
void connection_complete_async_io(struct connection *conn)
{
	int rc;

	conn->state = STATE_SENDING_DATA;
	conn->send_pos = 0;

	rc = w_epoll_add_ptr_out(epollfd, conn->sockfd, conn);
	if (rc < 0)
		goto close_connection;

	rc = w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	if (rc < 0)
		goto close_connection;

	rc = io_destroy(conn->ctx);
	if (rc < 0)
		goto close_connection;
	conn->ctx = 0;

	dlog(LOG_DEBUG, "Async IO completed\n");
	return;

close_connection:
	dlog(LOG_ERR, "Error completing async IO. Closing connection\n");
	connection_remove(conn);
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
	const size_t bytes_parsed =
		http_parser_execute(&conn->request_parser, &settings_on_path,
							conn->recv_buffer, conn->recv_len);

	dlog(LOG_DEBUG, "Parsed HTTP request, bytes: %lu, path: %s\n", bytes_parsed,
		 conn->request_path);
	return bytes_parsed;
}

/**
 * Return number of ready events for a
 * given connection or 0 if no events are ready.
 */
static inline eventfd_t read_eventfd(struct connection *conn)
{
	eventfd_t nr;
	int rc = read(conn->eventfd, &nr, sizeof(nr));

	if (rc == sizeof(nr))
		return nr;
	return 0;
}

/**
 * Get the results of `conn`'s asynchronous operations.
 * The number of events is given by `nr`.
 */
void get_io_event_results(struct connection *conn, eventfd_t nr)
{
	struct io_event events[MAXEVENTS];
	int rc;

	while (nr) {
		rc = io_getevents(conn->ctx, 1, MAXEVENTS, events, 0);
		if (rc < 0) {
			dlog(LOG_ERR, "Failed retrieving events. Closing connection\n");
			connection_remove(conn);
			return;
		}

		const int event_no = rc;

		for (int i = 0; i < event_no; i++) {
			if (events[i].res < 0) {
				dlog(LOG_ERR, "Error in async IO. Closing connection\n");
				connection_remove(conn);
				return;
			}

			conn->async_read_len += events[i].res;
			if (conn->async_read_len == conn->send_len)
				/* Buffer was sent completely. */
				connection_complete_async_io(conn);
		}

		nr -= event_no;
	}
}

/**
 * Handle input information: may be a new message or notification of
 * completion of an asynchronous I/O operation.
 */
void handle_input(struct connection *conn)
{
	const eventfd_t nr = read_eventfd(conn);

	if (nr) {
		get_io_event_results(conn, nr);
		return;
	}

	dlog(LOG_INFO, "New message received\n");
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
		/* Send the header data. */
		rc = connection_send_data(conn);
		if (rc < 0) {
			dlog(LOG_ERR, "Error sending package. Closing connection\n");
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
		dlog(LOG_INFO, "Header sent\n");

		if (conn->res_type == RESOURCE_TYPE_STATIC)
			connection_send_static(conn);
		else
			connection_start_async_io(conn);

		break;

	/* Sending the file. */
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
	if (event & EPOLLIN)
		handle_input(conn);
	else if (event & EPOLLOUT)
		handle_output(conn);
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
