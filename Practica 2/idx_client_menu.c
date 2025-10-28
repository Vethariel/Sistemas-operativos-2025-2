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
    // Crea un socket TCP (IPv4) para establecer conexión con el servidor
    int s = socket(AF_INET, SOCK_STREAM, 0);
    // Si no se puede crear el socket, muestra error y retorna -1
    if (s < 0)
    {
        perror("socket");
        return -1;
    }

    // Prepara la estructura de dirección del servidor (familia IPv4 y puerto)
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    // Convierte la dirección IP en texto al formato binario requerido por la red
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1)
    {
        fprintf(stderr, "IP inválida\n");
        close(s);
        return -1;
    }

    // Intenta establecer la conexión TCP con el servidor especificado
    if (connect(s, (struct sockaddr *)&sa, sizeof(sa)) < 0)
    {
        perror("connect");
        close(s);
        return -1;
    }

    // Devuelve el descriptor del socket si la conexión fue exitosa
    return s;
}

int main(int argc, char **argv)
{
    // Verifica que el usuario haya especificado host y puerto; si no, muestra uso y sale
    if (argc < 3)
    {
        fprintf(stderr, "Uso: %s <host> <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    // Obtiene la dirección del servidor y el puerto desde los argumentos
    const char *host = argv[1];
    int port = atoi(argv[2]);

    // Intenta conectar con el servidor TCP; si falla, termina el programa
    int sock = connect_server(host, port);
    if (sock < 0)
        return EXIT_FAILURE;

    // Muestra mensaje de confirmación al establecer la conexión correctamente
    printf("\nConectado al servidor %s:%d\n", host, port);

    // Bucle principal del menú interactivo; se repite hasta que el usuario elija salir
    for (;;)
    {
        // Muestra las opciones disponibles al usuario
        printf("\n=== MENÚ ===\n");
        printf("1. Consultar libro por ID\n");
        printf("2. Salir\n");
        printf("3. Añadir nuevo libro\n");
        printf("Seleccione una opción: ");

        // Lee la opción seleccionada; si hay entrada inválida, limpia el buffer y vuelve al menú
        int opcion = 0;
        if (scanf("%d", &opcion) != 1)
        {
            while (getchar() != '\n')
                ; // limpiar stdin
            continue;
        }

        // Si el usuario elige salir, envía comando QUIT al servidor y termina el bucle
        if (opcion == 2)
        {
            send(sock, "QUIT\n", 5, 0);
            break;
        }

        // Si el usuario elige añadir un nuevo libro, solicita los datos y envía el comando ADD
        if (opcion == 3)
        {
            // Limpia cualquier carácter pendiente en el buffer antes de leer nueva entrada
            while (getchar() != '\n')
                ; // limpiar stdin

            // Encabezado descriptivo del modo "añadir libro"
            printf("\n=== 🆕 Añadir nuevo libro ===\n");
            printf("Debe ingresar **todos los campos separados por coma** en el orden exacto siguiente.\n");
            printf("Cada campo se describe brevemente:\n\n");

            // Muestra la descripción y el orden exacto de los 22 campos esperados por el servidor
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

            // Muestra un ejemplo de entrada correcta con todos los campos llenos
            printf("👉 Ejemplo de entrada completa:\n");
            printf("5107,total:2610840,5:891037,1,The Catcher in the Rye,30,4:808278,1:133165,2:224884,44046,J.D. Salinger,3:553476,2001,book500k-600k.csv,Back Bay Books,eng,0316769177,The hero-narrator of The Catcher in the Rye...,3.8,277,55539,\n\n");

            // Advierte sobre el manejo correcto de campos vacíos (mantener las comas)
            printf("💡 Nota: si un campo no aplica, déjelo vacío pero conserve la coma.\n");
            printf("Por ejemplo: 200011,total:20,5:8,4,Another Book,10,4:6,1:2,2:1,3,Jane Doe,3:3,2023,file.csv,Publisher,,ISBN,,3.8,180.0,,\n\n");
            
            // Lee la línea completa introducida por el usuario y elimina el salto de línea final
            printf("👉 Ingrese la línea completa:\n");

            char line[4096];
            if (!fgets(line, sizeof(line), stdin))
            {
                printf("Error de entrada.\n");
                continue;
            }
            line[strcspn(line, "\n")] = 0; // quitar salto

            // Extrae el ID inicial de la línea CSV para mostrar un mensaje identificativo
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

            // Mensaje de confirmación visual antes de enviar el nuevo registro al servidor
            printf("\n📤 Enviando registro con ID %s al servidor...\n", idbuf);

            // Construye el comando "ADD <línea>" y lo envía al servidor mediante el socket TCP
            char cmd[5000];
            snprintf(cmd, sizeof(cmd), "ADD %s\n", line);
            send(sock, cmd, strlen(cmd), 0);

            // Espera la respuesta del servidor después de enviar el comando ADD
            char buf[BUF_SIZE];
            ssize_t n = recv(sock, buf, sizeof(buf) - 1, 0);
            // Si no se recibe respuesta válida, informa error y termina la conexión
            if (n <= 0)
            {
                printf("❌ Conexión cerrada o error.\n");
                break;
            }
            buf[n] = '\0';

            // Muestra el mensaje de respuesta del servidor (éxito o error) y vuelve al menú
            printf("\n--- RESPUESTA DEL SERVIDOR ---\n%s\n", buf);
            continue;
        }

        // Verifica que la opción seleccionada sea 1; si no lo es, muestra error y regresa al menú
        if (opcion != 1)
        {
            printf("Opción inválida.\n");
            continue;
        }

        // Solicita al usuario el identificador único del libro a consultar
        printf("Ingrese el ID del libro: ");
        // Lee el ID como número entero; si la entrada no es válida, limpia el buffer y vuelve al menú
        unsigned long long id;
        if (scanf("%llu", &id) != 1)
        {
            while (getchar() != '\n')
                ;
            printf("Entrada inválida.\n");
            continue;
        }

        // Limpia caracteres sobrantes en el buffer de entrada para evitar lecturas inconsistentes
        while (getchar() != '\n')
            ;

        // Construye el comando "GET <id>" y lo envía al servidor por el socket
        char cmd[64];
        snprintf(cmd, sizeof(cmd), "GET %llu\n", id);
        send(sock, cmd, strlen(cmd), 0);

        // Espera la respuesta del servidor (ficha del libro o mensaje de error)
        char buf[BUF_SIZE];
        ssize_t n = recv(sock, buf, sizeof(buf) - 1, 0);
        // Si la conexión se pierde o hay error, notifica y termina la sesión
        if (n <= 0)
        {
            printf("Conexión cerrada o error.\n");
            break;
        }
        // Finaliza la cadena recibida y muestra la respuesta formateada del servidor
        buf[n] = '\0';

        // Mostrar resultado
        printf("\n--- RESPUESTA DEL SERVIDOR ---\n%s\n", buf);
    }

    // Cierra el socket TCP al finalizar la sesión
    close(sock);
    printf("Desconectado.\n");
    // Termina el programa exitosamente
    return 0;
}
