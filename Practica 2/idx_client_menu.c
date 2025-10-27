#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BUF_SIZE 16384

int connect_server(const char *host, int port) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) { perror("socket"); return -1; }

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) {
        fprintf(stderr, "IP inválida\n");
        close(s);
        return -1;
    }

    if (connect(s, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        perror("connect");
        close(s);
        return -1;
    }

    return s;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <host> <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *host = argv[1];
    int port = atoi(argv[2]);

    int sock = connect_server(host, port);
    if (sock < 0) return EXIT_FAILURE;

    printf("\nConectado al servidor %s:%d\n", host, port);

    for (;;) {
        printf("\n=== MENÚ ===\n");
        printf("1. Consultar libro por ID\n");
        printf("2. Salir\n");
        printf("Seleccione una opción: ");

        int opcion = 0;
        if (scanf("%d", &opcion) != 1) {
            while (getchar() != '\n'); // limpiar stdin
            continue;
        }

        if (opcion == 2) {
            send(sock, "QUIT\n", 5, 0);
            break;
        }

        if (opcion != 1) {
            printf("Opción inválida.\n");
            continue;
        }

        printf("Ingrese el ID del libro: ");
        unsigned long long id;
        if (scanf("%llu", &id) != 1) {
            while (getchar() != '\n');
            printf("Entrada inválida.\n");
            continue;
        }

        // limpiar buffer stdin antes de enviar
        while (getchar() != '\n');

        // Enviar comando GET
        char cmd[64];
        snprintf(cmd, sizeof(cmd), "GET %llu\n", id);
        send(sock, cmd, strlen(cmd), 0);

        // Recibir respuesta
        char buf[BUF_SIZE];
        ssize_t n = recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            printf("Conexión cerrada o error.\n");
            break;
        }
        buf[n] = '\0';

        // Mostrar resultado
        printf("\n--- RESPUESTA DEL SERVIDOR ---\n%s\n", buf);
    }

    close(sock);
    printf("Desconectado.\n");
    return 0;
}
