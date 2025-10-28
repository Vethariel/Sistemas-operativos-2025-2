#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BUF_SIZE 16384

int connect_server(const char *host, int port)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0)
    {
        perror("socket");
        return -1;
    }

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1)
    {
        fprintf(stderr, "IP inválida\n");
        close(s);
        return -1;
    }

    if (connect(s, (struct sockaddr *)&sa, sizeof(sa)) < 0)
    {
        perror("connect");
        close(s);
        return -1;
    }

    return s;
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        fprintf(stderr, "Uso: %s <host> <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *host = argv[1];
    int port = atoi(argv[2]);

    int sock = connect_server(host, port);
    if (sock < 0)
        return EXIT_FAILURE;

    printf("\nConectado al servidor %s:%d\n", host, port);

    for (;;)
    {
        printf("\n=== MENÚ ===\n");
        printf("1. Consultar libro por ID\n");
        printf("2. Salir\n");
        printf("3. Añadir nuevo libro\n");
        printf("Seleccione una opción: ");

        int opcion = 0;
        if (scanf("%d", &opcion) != 1)
        {
            while (getchar() != '\n')
                ; // limpiar stdin
            continue;
        }

        if (opcion == 2)
        {
            send(sock, "QUIT\n", 5, 0);
            break;
        }

        if (opcion == 3)
        {
            while (getchar() != '\n')
                ; // limpiar stdin

            printf("\n=== 🆕 Añadir nuevo libro ===\n");
            printf("Debe ingresar **todos los campos separados por coma** en el orden exacto siguiente.\n");
            printf("Cada campo se describe brevemente:\n\n");

            printf("1. Id → Identificador único numérico del libro (sin repetir).\n");
            printf("2. RatingDistTotal → Total de calificaciones (ej: total:2610840).\n");
            printf("3. RatingDist5 → Cantidad de calificaciones con 5 estrellas (ej: 5:891037).\n");
            printf("4. PublishDay → Día de publicación (número entero).\n");
            printf("5. Name → Título completo del libro.\n");
            printf("6. PublishMonth → Mes de publicación (1–12, o 0 si no se conoce).\n");
            printf("7. RatingDist4 → Calificaciones con 4 estrellas (ej: 4:808278).\n");
            printf("8. RatingDist1 → Calificaciones con 1 estrella (ej: 1:133165).\n");
            printf("9. RatingDist2 → Calificaciones con 2 estrellas (ej: 2:224884).\n");
            printf("10. CountsOfReview → Número total de reseñas (numérico).\n");
            printf("11. Authors → Nombre(s) del autor o autores.\n");
            printf("12. RatingDist3 → Calificaciones con 3 estrellas (ej: 3:553476).\n");
            printf("13. PublishYear → Año de publicación (ej: 2001).\n");
            printf("14. source_file → Archivo fuente original (ej: book500k-600k.csv).\n");
            printf("15. Publisher → Editorial o casa publicadora.\n");
            printf("16. Language → Código de idioma (ej: eng, spa, en-GB, etc.).\n");
            printf("17. ISBN → Número estándar internacional del libro (ISBN10 o ISBN13).\n");
            printf("18. Description → Descripción o sinopsis (puede dejar vacío).\n");
            printf("19. Rating → Promedio general de calificaciones (ej: 3.8).\n");
            printf("20. pagesNumber → Número de páginas del libro (ej: 277).\n");
            printf("21. Count of text reviews → Número de reseñas escritas.\n");
            printf("22. PagesNumber → Campo redundante de páginas (mantener coma si vacío).\n\n");

            printf("👉 Ejemplo de entrada completa:\n");
            printf("5107,total:2610840,5:891037,1,The Catcher in the Rye,30,4:808278,1:133165,2:224884,44046,J.D. Salinger,3:553476,2001,book500k-600k.csv,Back Bay Books,eng,0316769177,The hero-narrator of The Catcher in the Rye...,3.8,277,55539,\n\n");

            printf("💡 Nota: si un campo no aplica, déjelo vacío pero conserve la coma.\n");
            printf("Por ejemplo: 200011,total:20,5:8,4,Another Book,10,4:6,1:2,2:1,3,Jane Doe,3:3,2023,file.csv,Publisher,,ISBN,,3.8,180.0,,\n\n");
            printf("👉 Ingrese la línea completa:\n");

            char line[4096];
            if (!fgets(line, sizeof(line), stdin))
            {
                printf("Error de entrada.\n");
                continue;
            }
            line[strcspn(line, "\n")] = 0; // quitar salto

            // extraer ID inicial para mostrar mensaje dinámico
            char idbuf[32];
            const char *comma = strchr(line, ',');
            if (comma)
            {
                size_t len = comma - line;
                if (len >= sizeof(idbuf))
                    len = sizeof(idbuf) - 1;
                memcpy(idbuf, line, len);
                idbuf[len] = '\0';
            }
            else
                strcpy(idbuf, "(desconocido)");

            printf("\n📤 Enviando registro con ID %s al servidor...\n", idbuf);

            // Armar el comando ADD
            char cmd[5000];
            snprintf(cmd, sizeof(cmd), "ADD %s\n", line);
            send(sock, cmd, strlen(cmd), 0);

            // Recibir respuesta
            char buf[BUF_SIZE];
            ssize_t n = recv(sock, buf, sizeof(buf) - 1, 0);
            if (n <= 0)
            {
                printf("❌ Conexión cerrada o error.\n");
                break;
            }
            buf[n] = '\0';

            printf("\n--- RESPUESTA DEL SERVIDOR ---\n%s\n", buf);
            continue;
        }

        if (opcion != 1)
        {
            printf("Opción inválida.\n");
            continue;
        }

        printf("Ingrese el ID del libro: ");
        unsigned long long id;
        if (scanf("%llu", &id) != 1)
        {
            while (getchar() != '\n')
                ;
            printf("Entrada inválida.\n");
            continue;
        }

        // limpiar buffer stdin antes de enviar
        while (getchar() != '\n')
            ;

        // Enviar comando GET
        char cmd[64];
        snprintf(cmd, sizeof(cmd), "GET %llu\n", id);
        send(sock, cmd, strlen(cmd), 0);

        // Recibir respuesta
        char buf[BUF_SIZE];
        ssize_t n = recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0)
        {
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
