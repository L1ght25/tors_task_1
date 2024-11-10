#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>
#include <pthread.h>

#define GATEWAY_PORT 12233
#define PORT 12345
#define BROADCAST_PORT 54321
#define BUFFER_SIZE 1024
#define NUM_SEGMENTS 10
#define RESPONSE_TIMEOUT 10
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
            printf("WORKER EXISTS ind %d: %s and %s!\n", i, inet_ntoa(workers[i].addr.sin_addr), inet_ntoa(from_addr.sin_addr));
            fflush(stdout);
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
            // printf("timeout...\n");
            // fflush(stdout);
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
        tv.tv_sec = TIMEOUT;
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

// TCP function to assign work
int send_request_to_worker(Worker *worker, Segment* segments, int ind) {
    int sock;
    char buffer[BUFFER_SIZE];
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    // set timeout
    struct timeval tv;
    tv.tv_sec = TIMEOUT;
    tv.tv_usec = 0;
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv) < 0) {
        perror("setsockopt");
        close(sock);
        exit(EXIT_FAILURE);
    }

    if (connect(sock, (struct sockaddr *)&worker->addr, sizeof(worker->addr)) < 0) {
        perror("connect");
        close(sock);
        printf("WTF, unable to connect\n");
        fflush(stdout);
        worker->available = 0;
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
    for (int i = 0; i < NUM_SEGMENTS; ++i) {
        int worker_sock = find_worker(segments, i);
        if (worker_sock == -1) {
            return -1;
        }

        waiters[i] = worker_sock;
    }

    return 0;
}

int receive_result(int worker_sock, double *result) {
    char buffer[BUFFER_SIZE];

    int recv_len = recv(worker_sock, buffer, BUFFER_SIZE, 0);
    // printf("got buffer: %s, length %d\n", buffer, recv_len);
    if (recv_len > 0) {
        buffer[recv_len] = '\0';
        fflush(stdout);
        int segment_ind;
        double segment_result;
        sscanf(buffer, "%d %lf", &segment_ind, &segment_result);
        *result += segment_result;
    }
    // printf("got length %d\n", recv_len);
    // fflush(stdout);
    close(worker_sock);

    return recv_len;
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

    while (1) {
        pthread_mutex_lock(&worker_mutex);

        int waiters[NUM_SEGMENTS];
        if (assign_segments(segments, waiters) == -1) {
            printf("No active workers, will retry...\n");
            exit(1);
            pthread_mutex_unlock(&worker_mutex);
            continue;
        }

        int handled_segments = 0;
        while (handled_segments != NUM_SEGMENTS) {
            int all_workers_are_dead = 0;
            for (int w = 0; w < NUM_SEGMENTS; ++w) {
                if (waiters[w] == -1) {
                    continue;
                }

                int bytes_readen = receive_result(waiters[w], &result);
                if (bytes_readen > 0) {
                    waiters[w] = -1;
                    handled_segments++;
                } else {
                    printf("Rerequest segment %d\n", w);
                    int new_sock = find_worker(segments, w);
                    if (new_sock == -1) {
                        all_workers_are_dead = 1;
                        break;
                    }
                    waiters[w] = new_sock;
                }
            }

            if (all_workers_are_dead) {
                printf("No active workers, will retry...\n");
                break;
            }
        }

        if (handled_segments == NUM_SEGMENTS) {
            pthread_mutex_unlock(&worker_mutex);
            break;
        }

        pthread_mutex_unlock(&worker_mutex);
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
