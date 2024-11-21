#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <errno.h>
#include <pthread.h>
#include <fcntl.h>
#include <poll.h>

#define GATEWAY_PORT 12233
#define PORT 12345
#define BROADCAST_PORT 54321
#define BUFFER_SIZE 1024
#define NUM_SEGMENTS 10
#define MAX_EVENTS 1024
#define RESPONSE_TIMEOUT 2
#define DISCOVER_INTERVAL 10

#define TIMEOUT 5

typedef struct {
    double start;
    double end;
} Segment;

typedef struct {
    struct sockaddr_in addr;
    int available;
} Worker;

Worker workers[NUM_SEGMENTS];
// int num_workers = 0;
int last_active_worker = -1;
pthread_mutex_t worker_mutex = PTHREAD_MUTEX_INITIALIZER;

void discover_send(int sock, char * msg, struct sockaddr *broadcast_addr, int size_if_broadcast_addr) {
    if (sendto(sock, msg, strlen(msg), 0, broadcast_addr, size_if_broadcast_addr) < 0) {
        perror("sendto");
        close(sock);
        exit(EXIT_FAILURE);
    }
    printf("Broadcast message sent\n");
    fflush(stdout);
}

int worker_exists(struct sockaddr_in from_addr) {
    for (int i = 0; i < NUM_SEGMENTS; ++i) {
        if (workers[i].addr.sin_addr.s_addr == from_addr.sin_addr.s_addr) {
            printf("WORKER EXISTS ind %d: %s and %s! Setting available\n", i, inet_ntoa(workers[i].addr.sin_addr), inet_ntoa(from_addr.sin_addr));
            fflush(stdout);

            workers[i].available = 1;
            return 1;
        }
    }
    return 0;
}

void new_worker(struct sockaddr_in from_addr) {
    for (int i = 0; i < NUM_SEGMENTS; ++i) {
        if (!workers[i].available) {
            workers[i].available = 1;
            workers[i].addr = from_addr;
            return;
        }
    }
}

void discover_wait(int sock) {
    char buffer[BUFFER_SIZE];
    struct sockaddr_in from_addr;
    socklen_t from_len = sizeof(from_addr);
    while (1) {
        // printf("try to receive...\n");
        // fflush(stdout);
        int recv_len = recvfrom(sock, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&from_addr, &from_len);
        if (recv_len == -1 && errno == EAGAIN) {
            printf("timeout...\n");
            fflush(stdout);
            return;
        }
        // printf("received!\n");
        // fflush(stdout);
        if (recv_len > 0) {
            buffer[recv_len] = '\0';
            if (strcmp(buffer, "READY") == 0) {
                from_addr.sin_port = htons(PORT);
                pthread_mutex_lock(&worker_mutex);
                if (!worker_exists(from_addr)) {
                    new_worker(from_addr);
                    printf("New worker discovered at %s:%d\n", inet_ntoa(from_addr.sin_addr), ntohs(from_addr.sin_port));
                    fflush(stdout);
                    // num_workers++;
                }
                pthread_mutex_unlock(&worker_mutex);
            }
        }
    }
}

// Broadcast function to discover workers
void *discover_thread_func() {
    printf("starting broadcast discover func...\n");
    fflush(stdout);

    while (1) {
        int sock;
        struct sockaddr_in broadcast_addr;
        char *msg = "DISCOVER";

        if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
            perror("socket");
            exit(EXIT_FAILURE);
        }

        int broadcast = 1;
        if (setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast)) < 0) {
            perror("setsockopt");
            close(sock);
            exit(EXIT_FAILURE);
        }

        // set timeout
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv) < 0) {
            perror("setsockopt");
            close(sock);
            exit(EXIT_FAILURE);
        }

        memset(&broadcast_addr, 0, sizeof(broadcast_addr));
        broadcast_addr.sin_family = AF_INET;
        broadcast_addr.sin_port = htons(BROADCAST_PORT);
        broadcast_addr.sin_addr.s_addr = htonl(INADDR_BROADCAST);

        discover_send(sock, msg, (struct sockaddr *)&broadcast_addr, sizeof(broadcast_addr));
        discover_wait(sock);

        close(sock);

        sleep(DISCOVER_INTERVAL);
    }
}

int connect_with_timeout(int sockfd, const struct sockaddr *addr, socklen_t addrlen, unsigned int timeout_ms) {
    int rc = 0;
    // Set O_NONBLOCK
    int sockfd_flags_before;
    if((sockfd_flags_before=fcntl(sockfd,F_GETFL,0)<0)) return -1;
    if(fcntl(sockfd,F_SETFL,sockfd_flags_before | O_NONBLOCK)<0) return -1;
    // Start connecting (asynchronously)
    do {
        if (connect(sockfd, addr, addrlen)<0) {
            // Did connect return an error? If so, we'll fail.
            if ((errno != EWOULDBLOCK) && (errno != EINPROGRESS)) {
                rc = -1;
            }
            // Otherwise, we'll wait for it to complete.
            else {
                // Set a deadline timestamp 'timeout' ms from now (needed b/c poll can be interrupted)
                struct timespec now;
                if(clock_gettime(CLOCK_MONOTONIC, &now)<0) { rc=-1; break; }
                struct timespec deadline = { .tv_sec = now.tv_sec,
                                             .tv_nsec = now.tv_nsec + timeout_ms*1000000l};
                // Wait for the connection to complete.
                do {
                    // Calculate how long until the deadline
                    if(clock_gettime(CLOCK_MONOTONIC, &now)<0) { rc=-1; break; }
                    int ms_until_deadline = (int)(  (deadline.tv_sec  - now.tv_sec)*1000l
                                                  + (deadline.tv_nsec - now.tv_nsec)/1000000l);
                    if(ms_until_deadline<0) { rc=0; break; }
                    // Wait for connect to complete (or for the timeout deadline)
                    struct pollfd pfds[] = { { .fd = sockfd, .events = POLLOUT } };
                    rc = poll(pfds, 1, ms_until_deadline);
                    // If poll 'succeeded', make sure it *really* succeeded
                    if(rc>0) {
                        int error = 0; socklen_t len = sizeof(error);
                        int retval = getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &len);
                        if(retval==0) errno = error;
                        if(error!=0) rc=-1;
                    }
                }
                // If poll was interrupted, try again.
                while(rc==-1 && errno==EINTR);
                // Did poll timeout? If so, fail.
                if(rc==0) {
                    errno = ETIMEDOUT;
                    rc=-1;
                }
            }
        }
    } while(0);
    // Restore original O_NONBLOCK state
    if(fcntl(sockfd,F_SETFL,sockfd_flags_before)<0) return -1;
    // Success
    return rc;
}

// TCP function to assign work
int send_request_to_worker(Worker *worker, Segment* segments, int ind) {
    int sock;
    char buffer[BUFFER_SIZE];
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    // // set timeout
    // struct timeval tv;
    // tv.tv_sec = TIMEOUT;
    // tv.tv_usec = 0;
    // if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv) < 0) {
    //     perror("setsockopt");
    //     close(sock);
    //     exit(EXIT_FAILURE);
    // }

    if (connect_with_timeout(sock, (struct sockaddr *)&worker->addr, sizeof(worker->addr), TIMEOUT) < 0) {
        perror("connect");
        close(sock);
        printf("WTF, unable to connect\n");
        fflush(stdout);
        worker->available = 0;
        return -1;
    }

    printf("sending request to worker %s\n", inet_ntoa(worker->addr.sin_addr));
    fflush(stdout);
    snprintf(buffer, BUFFER_SIZE, "%d %lf %lf", ind, segments[ind].start, segments[ind].end);
    send(sock, buffer, strlen(buffer), 0);

    return sock;
}

int find_worker(Segment* segments, int ind) {
    int worker_was_found = 0;
    for (int w = last_active_worker + 1; w <= last_active_worker + NUM_SEGMENTS; ++w) {
        int curr_worker = w % NUM_SEGMENTS;
        if (!workers[curr_worker].available) {
            continue;
        }

        int sock = send_request_to_worker(&workers[curr_worker], segments, ind);
        if (workers[curr_worker].available) {
            last_active_worker = curr_worker;
            printf("assigned segment %d to worker %d\n", ind, curr_worker);
            fflush(stdout);
            return sock;
        }
    }
    printf("!!! unassigned segment %d", ind);
    fflush(stdout);
    return -1;
}

int assign_segments(Segment *segments, int *waiters) {
    pthread_mutex_lock(&worker_mutex);
    for (int i = 0; i < NUM_SEGMENTS; ++i) {
        // already handled
        if (waiters[i] == -1) {
            continue;
        }

        int worker_sock = find_worker(segments, i);
        if (worker_sock == -1) {
            pthread_mutex_unlock(&worker_mutex);
            return -1;
        }

        waiters[i] = worker_sock;
    }

    pthread_mutex_unlock(&worker_mutex);
    return 0;
}

int receive_result(int *workers, int sock, double *result) {
    char buffer[BUFFER_SIZE];

    int recv_len = recv(sock, buffer, BUFFER_SIZE, 0);
    printf("got buffer: %s, length %d\n", buffer, recv_len);
    if (recv_len > 0) {
        buffer[recv_len] = '\0';
        fflush(stdout);
        int segment_ind;
        double segment_result;
        sscanf(buffer, "%d %lf", &segment_ind, &segment_result);
        *result += segment_result;

        workers[segment_ind] = -1;
    }
    printf("got length %d\n", recv_len);
    fflush(stdout);

    return recv_len;
}

int receive_results_with_epoll(int *worker_sockets, double *result) {
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }

    int handled_segments = 0;
    struct epoll_event event, events[MAX_EVENTS];
    for (int i = 0; i < NUM_SEGMENTS; ++i) {
        if (worker_sockets[i] != -1) {
            // Добавляем сокеты в kqueue для мониторинга
            event.data.fd = worker_sockets[i];
            event.events = EPOLLIN;
            if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, worker_sockets[i], &event) == -1) {
                perror("epoll_ctl");
                close(epoll_fd);
                return -1;
            }
        } else {
            handled_segments++;
        }
    }

    while (handled_segments < NUM_SEGMENTS) {
        printf("WAITING...: %d\n", handled_segments);
        fflush(stdout);
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, RESPONSE_TIMEOUT * 1000);
        if (n == -1) {
            if (errno == EINTR) continue;
            perror("epoll");
            break;
        } else if (n == 0) {
            // Таймаут истёк
            printf("epoll timeout: some workers did not respond.\n");
            break;
        }

        for (int i = 0; i < n; ++i) {
            printf("got epoll event, iter %d\n", i);
            fflush(stdout);
            int sock = events[i].data.fd;

            if (!(events[i].events & EPOLLIN)) {
                fprintf(stderr, "EPOLLIN error on socket %d\n", sock);
            } else {
                int bytes_readen = receive_result(worker_sockets, sock, result);
                if (bytes_readen > 0) {
                    handled_segments++;
                    printf("HANDLED: %d\n", handled_segments);
                    fflush(stdout);
                } else {
                    // Ошибка чтения или разрыв соединения
                    perror("recv");
                }
            }

            close(sock);
        }
    }

    // Закрываем сокеты, которые не успели ответить
    for (int i = 0; i < NUM_SEGMENTS; ++i) {
        if (worker_sockets[i] != -1) {
            printf("Worker socket %d did not respond in time, closing.\n", worker_sockets[i]);
            close(worker_sockets[i]);
            // worker_sockets[i] = -1; // Обозначаем как необработанный
        }
    }

    close(epoll_fd);
    return handled_segments == NUM_SEGMENTS ? 0 : -1;
}

double compute_request(double start, double end) {
    double length = end - start;
    double result = 0;
    Segment segments[NUM_SEGMENTS];

    // Divide the integration range [start, end] into NUM_SEGMENTS segments
    for (int i = 0; i < NUM_SEGMENTS; i++) {
        segments[i].start = start + length * i / NUM_SEGMENTS;
        segments[i].end = start + length * (i + 1) / NUM_SEGMENTS;
    }

    int waiters[NUM_SEGMENTS];
    while (1) {
        if (assign_segments(segments, waiters) == -1) {
            printf("No active workers, aborting...\n");
            result = -1;
            break;
        }

        if (receive_results_with_epoll(waiters, &result) == 0) {
            break;
        }

        printf("Cannot receive, will retry...\n");
    }

    return result;
}

// Функция для обработки локальных запросов
void handle_local_request(int client_socket) {
    char buffer[1024];
    ssize_t bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received <= 0) {
        close(client_socket);
        return;
    }

    buffer[bytes_received] = '\0'; // Завершаем строку
    printf("Received local request: %s\n", buffer);

    double start, end;
    if (sscanf(buffer, "%lf %lf", &start, &end) < 0) {
        snprintf(buffer, sizeof(buffer), "Invalid request");
        send(client_socket, buffer, strlen(buffer), 0);
        close(client_socket);
        return;
    }
    double result = compute_request(start, end);

    // Отправляем результат в текстовом формате
    snprintf(buffer, sizeof(buffer), "Integral result: %.6f", result);
    send(client_socket, buffer, strlen(buffer), 0);

    // clear
    last_active_worker = -1;

    close(client_socket);

    printf("Computed request, closing...: %d\n", client_socket);
    fflush(stdout);
}

// Функция для прослушивания локальных запросов
void listen_for_local_requests() {
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(GATEWAY_PORT);
    
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    
    if (listen(server_fd, 3) < 0) {
        perror("Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Waiting for local requests on port %d...\n", GATEWAY_PORT);
    
    while (1) {
        int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (new_socket < 0) {
            perror("Accept failed");
            continue;
        }
        
        // Обработка локального запроса
        handle_local_request(new_socket);
    }
    
    close(server_fd);
}

void init() {
    for (int i = 0; i < NUM_SEGMENTS; ++i) {
        workers[i].available = 0;
        memset(&workers[i].addr, 0, sizeof(workers[i].addr));
    }
}

// Main function
int main() {
    init();

    pthread_t discover_thread;
    pthread_create(&discover_thread, NULL, discover_thread_func, NULL);

    // sleep(3); // Give workers time to respond

    listen_for_local_requests();

    pthread_cancel(discover_thread);
    return 0;
}
