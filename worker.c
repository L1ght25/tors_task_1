#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <math.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>


#define PORT 12345
#define BROADCAST_PORT 54321
#define BUFFER_SIZE 1024

double compute_integral(double start, double end) {
    int steps = 1000;
    double step = (end - start) / steps;
    double sum = 0;
    for (int i = 0; i < steps; i++) {
        double x = start + i * step;
        sum += x * x * step;
    }
    return sum;
}

// void respond_discovery() {
//     int sock;
//     struct sockaddr_in broadcast_addr;
//     char *msg = "READY";

//     if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
//         perror("socket");
//         exit(EXIT_FAILURE);
//     }

//     broadcast_addr.sin_family = AF_INET;
//     broadcast_addr.sin_port = htons(BROADCAST_PORT);
//     broadcast_addr.sin_addr.s_addr = inet_addr("255.255.255.255");

//     sendto(sock, msg, strlen(msg), 0, (struct sockaddr *)&broadcast_addr, sizeof(broadcast_addr));
//     close(sock);
// }

void *wait_for_discover() {
    int sock;
    struct sockaddr_in server_addr, master_addr;
    char buffer[BUFFER_SIZE];
    socklen_t addr_len = sizeof(master_addr);

    // Создаем UDP сокет
    if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    // Настраиваем адрес для прослушивания
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(BROADCAST_PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    // Привязываем сокет к адресу
    if (bind(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(sock);
        exit(EXIT_FAILURE);
    }

    printf("Worker waiting for DISCOVER message...\n");
    fflush(stdout);

    // Ждем сообщения DISCOVER от мастера
    while (1) {
        int recv_len = recvfrom(sock, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&master_addr, &addr_len);
        if (recv_len > 0) {
            buffer[recv_len] = '\0';
            if (strcmp(buffer, "DISCOVER") == 0) {
                printf("Received DISCOVER message from master, sending READY response...\n");
                fflush(stdout);
                
                // Отправляем сообщение READY мастеру
                const char *msg = "READY";
                if (sendto(sock, msg, strlen(msg), 0, (struct sockaddr *)&master_addr, addr_len) < 0) {
                    perror("sendto");
                }
                // break; // Выходим из цикла после ответа мастеру
            }
        }
    }

    close(sock);
}

// TCP handler for tasks from master
void handle_tasks() {
    int sock, new_sock;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    char buffer[BUFFER_SIZE];

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(sock);
        exit(EXIT_FAILURE);
    }

    listen(sock, 5);
    printf("Worker is ready to receive tasks...\n");

    while (1) {
        new_sock = accept(sock, (struct sockaddr *)&client_addr, &client_addr_len);
        if (new_sock < 0) {
            perror("accept");
            continue;
        }

        int recv_len = recv(new_sock, buffer, BUFFER_SIZE, 0);
        if (recv_len > 0) {
            buffer[recv_len] = '\0';
            printf("Received %s, length %d\n", buffer, recv_len);
            fflush(stdout);
            int segment_ind;
            double start, end;
            sscanf(buffer, "%d %lf %lf", &segment_ind, &start, &end);

            double result = compute_integral(start, end);
            snprintf(buffer, BUFFER_SIZE, "%d %f", segment_ind, result);
            printf("Sending %s, length %d\n", buffer, recv_len);
            fflush(stdout);
            send(new_sock, buffer, strlen(buffer), 0);
        }
        close(new_sock);
    }
}

int main() {
    pthread_t discover_thread;
    pthread_create(&discover_thread, NULL, wait_for_discover, NULL);
    handle_tasks();
    pthread_cancel(discover_thread);
    return 0;
}
