#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <signal.h>  // For signal handling
#include "routes.h"
#include "server.h"  // Include this for function declaration
#include "../lib/db.c"

#define BUFFER_SIZE 1024

int server_fd;  // Global variable to store the server socket descriptor

void handle_sigint(int sig) {
    printf("\n=> Shutting down server...\n", sig);
    if (server_fd >= 0) {
        close(server_fd);  // Close the server socket
    }
    exit(0);  // Exit the program
}

void start_server(void) {
    // Create necessary directories
    initialize();
    int new_socket;
    struct sockaddr_in address;
    int addrlen = sizeof(address);
    char buffer[BUFFER_SIZE] = {0};
    int PORT = 3232;
    char *username = malloc(256);
    char *password = malloc(256);

    // Open the config file for reading
    FILE *file = fopen("config", "r");
    if (file == NULL) {
        perror("Error opening config file");
        return;
    }

    char line[256];
    while (fgets(line, sizeof(line), file)) {
        if (strstr(line, "port") != NULL) {
            PORT = atoi(trim(replaceString(line, "port=", "")));  // Correctly assign port as an integer
        }
        if (strstr(line, "username") != NULL) {
            strcpy(username, trim(replaceString(line, "username=", "")));  // Copy string to username
        }
        if (strstr(line, "password") != NULL) {
            strcpy(password, trim(replaceString(line, "password=", "")));  // Copy string to password
        }
    }
    fclose(file);

    // Trim Newlines
    trim_newlines(username);
    trim_newlines(password);

    // Setup signal handler for SIGINT
    signal(SIGINT, handle_sigint);

    // Create socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket failed");
        exit(EXIT_FAILURE);
    }

    // Bind the socket to the network address and port
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for incoming connections
    if (listen(server_fd, 3) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("=> Simple DB Started on port %d\n", PORT);

    while (1) {
        // Accept incoming connection
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("Accept failed");
            exit(EXIT_FAILURE);
        }

        // Read the incoming request
        read(new_socket, buffer, BUFFER_SIZE);
        
        // Handle the request and send response
        handle_request(buffer, new_socket, username, password);

        // Close the socket
        close(new_socket);
        memset(buffer, 0, BUFFER_SIZE); // Clear buffer for the next request
    }
}

