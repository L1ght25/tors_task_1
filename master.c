// #include <cstdio>
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

#define PORT 12345
#define BROADCAST_PORT 54321
#define BUFFER_SIZE 1024
#define NUM_SEGMENTS 10
#define RESPONSE_TIMEOUT 10
#define DISCOVER_INTERVAL 10

typedef struct {
    double start;
    double end;
} Segment;

typedef struct {
    struct sockaddr_in addr;
    // int sock;
    int available;
} Worker;

// typedef struct {
//     int sock;
// } AssignedSegment;

Worker workers[NUM_SEGMENTS];
int num_workers = 0;
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

void discover_wait(int sock) {
    char buffer[BUFFER_SIZE];
    struct sockaddr_in from_addr;
    socklen_t from_len = sizeof(from_addr);
    while (1) {
        int recv_len = recvfrom(sock, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&from_addr, &from_len);
        if (recv_len > 0) {
            buffer[recv_len] = '\0';
            if (strcmp(buffer, "READY") == 0) {
                from_addr.sin_port = htons(PORT);
                pthread_mutex_lock(&worker_mutex);
                workers[num_workers].addr = from_addr;
                workers[num_workers].available = 1;
                num_workers++;
                pthread_mutex_unlock(&worker_mutex);
                printf("Worker discovered at %s:%d\n", inet_ntoa(from_addr.sin_addr), ntohs(from_addr.sin_port));
                fflush(stdout);
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

int assign_segment_to_worker(Segment* segments, int ind) {
    int worker_was_found = 0;
    for (int w = 0; w < num_workers; ++w) {
        if (!workers[w].available) {
            continue;
        }

        int sock = send_request_to_worker(&workers[w], segments, ind);
        if (workers[w].available) {
            printf("assigned segment %d to worker %d\n", ind, w);
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
        int worker_sock = assign_segment_to_worker(segments, i);
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
    printf("got buffer: %s, length %d\n", buffer, recv_len);
    if (recv_len > 0) {
        buffer[recv_len] = '\0';
        fflush(stdout);
        int segment_ind;
        double segment_result;
        sscanf(buffer, "%d %lf", &segment_ind, &segment_result);
        *result += segment_result;
    }
    printf("got length %d\n", recv_len);
    fflush(stdout);
    close(worker_sock);

    return recv_len;
}

// Main function
int main() {
    pthread_t discover_thread;
    pthread_create(&discover_thread, NULL, discover_thread_func, NULL);

    // broadcast_discover();
    sleep(3); // Give workers time to respond

    double result = 0;
    double segment_result;
    Segment segments[NUM_SEGMENTS];
    // Divide the integration range [0, 1] into NUM_SEGMENTS segments
    for (int i = 0; i < NUM_SEGMENTS; i++) {
        segments[i].start = (double)i / NUM_SEGMENTS;
        segments[i].end = (double)(i + 1) / NUM_SEGMENTS;
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
                    int new_sock = assign_segment_to_worker(segments, w);
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

    printf("Result of integral: %f\n", result);

    pthread_cancel(discover_thread);
    return 0;
}
