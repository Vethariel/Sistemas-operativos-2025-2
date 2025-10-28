#define _POSIX_C_SOURCE 200809L
#define _FILE_OFFSET_BITS 64
#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

// ====== Estructuras del índice ======
typedef struct
{
    uint64_t id;
    uint64_t offset;
} Pair;

typedef struct
{
    char magic[8];          // "BKIDXv01"
    uint64_t table_size;    // 1000
    uint64_t total_entries; // N
} Header;

typedef struct
{
    uint64_t bucket_offset; // desplazamiento en books.idx
    uint64_t bucket_count;  // nº de pares
} DirEntry;

static inline unsigned hash_id(uint64_t id)
{
    return (unsigned)((id * 2654435761UL) % 1000);
}

// ====== Estado global sólo-lectura ======
static FILE *g_idx = NULL;
static FILE *g_csv = NULL;
static Header g_hdr;
static DirEntry *g_dir = NULL;

static volatile sig_atomic_t g_stop = 0;
static void handle_sigint(int s)
{
    (void)s;
    g_stop = 1;
}

// ====== Lectura robusta de línea del socket ======
static ssize_t read_line(int fd, char *buf, size_t cap)
{
    size_t n = 0;
    while (n + 1 < cap)
    {
        char c;
        ssize_t r = recv(fd, &c, 1, 0);
        if (r == 0)
            return 0; // peer closed
        if (r < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }
        buf[n++] = c;
        if (c == '\n')
            break;
    }
    buf[n] = '\0';
    return (ssize_t)n;
}

// ====== Busca id en su bucket (carga bucket a RAM, <= unos cientos de KB) ======
static int find_offset(uint64_t id, uint64_t *out_off)
{
    unsigned b = hash_id(id);
    uint64_t count = g_dir[b].bucket_count;
    if (count == 0)
        return 0;

    if (fseeko(g_idx, (off_t)g_dir[b].bucket_offset, SEEK_SET) != 0)
        return -1;

    size_t bytes = (size_t)count * sizeof(Pair);
    // Seguridad de memoria: limita a 8MB por lectura de bucket (muy por debajo del límite pedido)
    if (bytes > (8u << 20))
        return -1;

    Pair *buf = (Pair *)malloc(bytes);
    if (!buf)
        return -1;

    size_t rd = fread(buf, sizeof(Pair), (size_t)count, g_idx);
    if (rd != (size_t)count)
    {
        free(buf);
        return -1;
    }

    // Binary search por id
    int lo = 0, hi = (int)count - 1;
    while (lo <= hi)
    {
        int mid = lo + ((hi - lo) >> 1);
        if (buf[mid].id == id)
        {
            *out_off = buf[mid].offset;
            free(buf);
            return 1;
        }
        else if (buf[mid].id < id)
        {
            lo = mid + 1;
        }
        else
        {
            hi = mid - 1;
        }
    }
    free(buf);
    return 0; // no encontrado
}

// ====== Lee la línea completa del CSV en offset ======
static int read_csv_line_at(uint64_t off, char **out, size_t *out_len)
{
    if (fseeko(g_csv, (off_t)off, SEEK_SET) != 0)
        return -1;

    // Lee hasta '\n' en un buffer dinámico
    size_t cap = 4096;
    size_t len = 0;
    char *buf = (char *)malloc(cap);
    if (!buf)
        return -1;

    for (;;)
    {
        int c = fgetc(g_csv);
        if (c == EOF)
            break;
        if (len + 1 >= cap)
        {
            size_t ncap = cap * 2;
            char *tmp = (char *)realloc(buf, ncap);
            if (!tmp)
            {
                free(buf);
                return -1;
            }
            cap = ncap;
            buf = tmp;
        }
        buf[len++] = (char)c;
        if (c == '\n')
            break;
    }
    buf[len] = '\0';
    *out = buf;
    *out_len = len;
    return (len > 0) ? 0 : -1;
}

// ===============================================================
// Convierte una línea CSV en ficha legible (solo campos clave)
// ===============================================================
static char *format_record(const char *csv_line)
{
    // Duplicamos la línea porque strtok modifica el string
    char *temp = strdup(csv_line);
    if (!temp)
        return NULL;

    const int MAX_FIELDS = 24;
    char *fields[MAX_FIELDS];
    int count = 0;

    char *tok = strtok(temp, ",");
    while (tok && count < MAX_FIELDS)
    {
        fields[count++] = tok;
        tok = strtok(NULL, ",");
    }

    // Asignar variables con seguridad
    const char *id = (count > 0) ? fields[0] : "";
    const char *titulo = (count > 4) ? fields[4] : "";
    const char *autor = (count > 10) ? fields[10] : "";
    const char *editorial = (count > 14) ? fields[14] : "";
    const char *idioma = (count > 15) ? fields[15] : "";
    const char *anio = (count > 12) ? fields[12] : "";
    const char *rating = (count > 18) ? fields[18] : "";
    const char *paginas = (count > 19) ? fields[19] : "";
    const char *archivo = (count > 13) ? fields[13] : "";
    const char *descripcion = (count > 17) ? fields[17] : "";

    // Reservamos espacio para el texto final
    char *out = malloc(4096);
    if (!out)
    {
        free(temp);
        return NULL;
    }

    snprintf(out, 4096,
             "OK\n"
             "ID: %s\n"
             "Título: %s\n"
             "Autor: %s\n"
             "Editorial: %s\n"
             "Idioma: %s\n"
             "Año: %s\n"
             "Rating: %s\n"
             "Páginas: %s\n"
             "Archivo origen: %s\n"
             "Descripción: %s\n"
             "----------------------------------------\n",
             id, titulo, autor, editorial, idioma, anio,
             rating, paginas, archivo, descripcion);

    free(temp);
    return out;
}

// ====== Inserta un nuevo par (id, offset) directamente en el índice ======
static int insert_into_index(uint64_t id, uint64_t offset)
{
    unsigned b = hash_id(id);
    DirEntry *d = &g_dir[b];

    // Cargar el bucket actual
    if (fseeko(g_idx, (off_t)d->bucket_offset, SEEK_SET) != 0)
        return -1;

    Pair *pairs = malloc(sizeof(Pair) * (d->bucket_count + 1));
    if (!pairs)
        return -1;

    size_t rd = fread(pairs, sizeof(Pair), (size_t)d->bucket_count, g_idx);
    if (rd != (size_t)d->bucket_count && ferror(g_idx))
    {
        free(pairs);
        return -1;
    }

    // Insertar manteniendo orden por id
    size_t i = 0;
    while (i < d->bucket_count && pairs[i].id < id)
        i++;
    memmove(&pairs[i + 1], &pairs[i], (d->bucket_count - i) * sizeof(Pair));
    pairs[i].id = id;
    pairs[i].offset = offset;
    d->bucket_count++;

    // Escribir nuevo bloque al final del archivo
    fseeko(g_idx, 0, SEEK_END);
    d->bucket_offset = (uint64_t)ftello(g_idx);
    fwrite(pairs, sizeof(Pair), d->bucket_count, g_idx);
    fflush(g_idx);
    free(pairs);

    // Actualizar el directorio
    fseeko(g_idx, sizeof(Header) + (b * sizeof(DirEntry)), SEEK_SET);
    fwrite(d, sizeof(DirEntry), 1, g_idx);
    fflush(g_idx);

    // Actualizar el header (total_entries)
    g_hdr.total_entries++;
    fseeko(g_idx, 0, SEEK_SET);
    fwrite(&g_hdr, sizeof(Header), 1, g_idx);
    fflush(g_idx);

    return 0;
}

// ====== Hilo por conexión ======
typedef struct
{
    int fd;
} ClientCtx;

static void *client_thread(void *arg)
{
    // Extrae el contexto del cliente pasado como argumento por el hilo
    ClientCtx *ctx = (ClientCtx *)arg;
    // Guarda el descriptor del socket del cliente para lectura/escritura
    int fd = ctx->fd;
    // Libera la estructura temporal del cliente (ya no se necesita)
    free(ctx);

    // Bucle principal: procesa comandos del cliente mientras la conexión esté abierta
    char line[256];
    // Lee líneas hasta que el cliente cierre o envíe QUIT
    for (;;)
    {
        // Lee una línea de comando del socket del cliente (terminada en '\n')
        ssize_t n = read_line(fd, line, sizeof(line));
        // Si el cliente cerró la conexión o hubo error, salir del bucle
        if (n <= 0)
            break;

        // Elimina el salto de línea final '\n' del comando recibido
        if (n > 0 && line[n - 1] == '\n')
            line[n - 1] = '\0';
        // Limpia un posible '\r' final (por compatibilidad con clientes Windows)
        size_t L = strlen(line);
        if (L && line[L - 1] == '\r')
            line[L - 1] = '\0';

        // Si el cliente envía 'QUIT', cerrar la conexión limpiamente
        if (strcasecmp(line, "QUIT") == 0)
            break;
        // Si el comando comienza con 'ADD ', procesar la inserción de un nuevo registro
        if (strncasecmp(line, "ADD ", 4) == 0)
        {
            // 1. Extraer línea CSV completa
            // Obtiene el texto del nuevo registro CSV (después de "ADD ") y omite espacios
            const char *csv_line = line + 4;
            // Saltar espacios iniciales
            while (*csv_line == ' ')
                csv_line++;

            // 2. Extraer el ID inicial
            char idbuf[32];
            // Busca la primera coma para separar el ID del resto del registro
            const char *comma = strchr(csv_line, ',');
            // Si no hay coma, el formato es incorrecto: enviar error al cliente
            if (!comma)
            {
                const char *msg = "ERR formato CSV inválido\n";
                send(fd, msg, strlen(msg), 0);
                continue;
            }
            // Copia el valor del ID (antes de la primera coma) en un buffer seguro
            size_t len = comma - csv_line;
            // Seguridad: limitar tamaño del ID
            if (len >= sizeof(idbuf))
                len = sizeof(idbuf) - 1;
            // Establecer el ID como cadena
            memcpy(idbuf, csv_line, len);
            // Añadir terminador nulo
            idbuf[len] = '\0';
            // Convierte el ID a número entero (uint64_t) para indexarlo
            uint64_t id = strtoull(idbuf, NULL, 10);

            // 3. Verificar si el ID ya existe
            uint64_t off_exist = 0;
            // Busca en el índice si el ID ya está registrado; devuelve 1 si existe, 0 si no
            int exists = find_offset(id, &off_exist);
            // Si hubo error al leer el índice, notificar al cliente y abortar esta operación
            if (exists < 0)
            {
                const char *msg = "ERR index read error\n";
                send(fd, msg, strlen(msg), 0);
                continue;
            }
            // Si el ID ya existe en el índice, enviar error de duplicado
            if (exists > 0)
            {
                const char *msg = "ERR ID duplicado\n";
                send(fd, msg, strlen(msg), 0);
                continue;
            }

            // 4. Escribir la nueva línea al final del CSV

            // Mueve el puntero al final del archivo CSV para agregar el nuevo registro
            fseeko(g_csv, 0, SEEK_END);
            // Obtiene el desplazamiento (offset) actual, para indexar este registro en el archivo
            uint64_t offset = (uint64_t)ftello(g_csv);
            // Escribe el nuevo registro CSV en el archivo con salto de línea final
            fprintf(g_csv, "%s\n", csv_line);
            // Fuerza la escritura inmediata al disco para mantener la coherencia del índice
            fflush(g_csv);

            // 5. Insertar en el índice binario

            // Inserta el nuevo par (ID, offset) en el índice binario; si falla, notificar error
            if (insert_into_index(id, offset) != 0)
            {
                const char *msg = "ERR inserción en índice\n";
                send(fd, msg, strlen(msg), 0);
                continue;
            }

            // 6. Confirmar al cliente

            // Envía confirmación al cliente de que el registro se insertó correctamente
            const char *okmsg = "OK Registro agregado correctamente\n";
            send(fd, okmsg, strlen(okmsg), 0);
            continue;
        }
        // Si el comando no es 'GET' ni 'ADD', enviar mensaje de error y continuar
        else if (strncasecmp(line, "GET ", 4) != 0)
        {
            const char *msg = "ERR expected: GET <id> or ADD <csv>\n";
            send(fd, msg, strlen(msg), 0);
            send(fd, "\n", 1, 0);
            continue;
        }

        // Salta la palabra 'GET ' y cualquier espacio extra antes del ID
        char *p = line + 4;
        while (*p == ' ')
            p++;
        // Si no hay ID después del comando GET, enviar error al cliente
        if (*p == '\0')
        {
            const char *msg = "ERR missing id\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        // Convierte el texto del ID a número entero (uint64_t), controlando errores
        errno = 0;
        char *endp = NULL;
        uint64_t id = strtoull(p, &endp, 10);
        // Si el ID no es numérico o excede el rango válido, notificar error al cliente
        if (errno == ERANGE || endp == p)
        {
            const char *msg = "ERR bad id\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        // Busca el ID en el índice binario; devuelve su desplazamiento en el CSV si existe
        uint64_t off = 0;
        int r = find_offset(id, &off);
        // Si ocurre un error interno al leer el índice, informar al cliente
        if (r < 0)
        {
            const char *msg = "ERR internal\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }
        // Si el ID no está en el índice, enviar mensaje 'NOTFOUND' al cliente
        if (r == 0)
        {
            const char *msg = "NOTFOUND\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        // Variables para almacenar la línea CSV leída desde el archivo
        char *csv_line = NULL;
        size_t csv_len = 0;
        // Lee desde el archivo CSV la línea completa ubicada en el offset indicado
        // Si ocurre un error de lectura, notificar al cliente
        if (read_csv_line_at(off, &csv_line, &csv_len) != 0)
        {
            const char *msg = "ERR readcsv\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        // Respuesta: "OK <nbytes>\n<linea>"
        // Genera una ficha legible a partir de la línea CSV y libera la memoria original
        char *ficha = format_record(csv_line);
        free(csv_line);

        // Si falló el formateo de la línea CSV, enviar error al cliente
        if (!ficha)
        {
            const char *msg = "ERR format\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        // Envía al cliente la ficha final del registro y libera la memoria usada
        send(fd, ficha, strlen(ficha), 0);
        free(ficha);
    }

    // Cierra el socket del cliente al finalizar la conexión
    close(fd);
    // Termina el hilo del cliente
    return NULL;
}

// ====== Main: servidor TCP ======
int main(int argc, char **argv)
{
    // Verifica que se hayan pasado los 4 argumentos requeridos (IP, puerto, índice y CSV)
    if (argc < 5)
    {
        // Muestra mensaje de uso correcto si faltan argumentos
        fprintf(stderr, "Uso: %s <bind_ip> <port> <books.idx> <books_validos.csv>\n", argv[0]);
        // Finaliza el programa indicando error de ejecución
        return EXIT_FAILURE;
    }

    // IP local donde el servidor escuchará (por ejemplo 127.0.0.1)
    const char *bind_ip = argv[1];
    // Convierte el argumento de puerto (cadena) a entero
    int port = atoi(argv[2]);
    // Rutas de los archivos del índice y del CSV de libros
    const char *idx_path = argv[3];
    const char *csv_path = argv[4];

    // Captura SIGINT (Ctrl+C) para cerrar el servidor limpiamente mediante handle_sigint()
    signal(SIGINT, handle_sigint);

    // Abrir índice y CSV

    // Abre el archivo de índice binario (.idx) en modo lectura/escritura ("r+b")
    g_idx = fopen(idx_path, "r+b"); // lectura/escritura binaria
    // Si no se puede abrir el índice, muestra error y termina el programa
    if (!g_idx)
    {
        perror("open idx");
        return EXIT_FAILURE;
    }

    // Abre el archivo CSV de libros en modo lectura/escritura con append ("a+b")
    g_csv = fopen(csv_path, "a+b"); // append + lectura
    // Si no se puede abrir el CSV, muestra error y termina el programa
    if (!g_csv)
    {
        perror("open csv");
        return EXIT_FAILURE;
    }

    // Leer header

    // Lee el encabezado (header) del archivo de índice binario
    if (fread(&g_hdr, sizeof(g_hdr), 1, g_idx) != 1)
    {
        // Si la lectura del header falla, muestra error y detiene el servidor
        perror("read header");
        return EXIT_FAILURE;
    }
    // Verifica que la firma "BKIDXv01" y el tamaño de tabla (1000) sean válidos
    if (memcmp(g_hdr.magic, "BKIDXv01", 8) != 0 || g_hdr.table_size != 1000)
    {
        // Si el índice no cumple el formato esperado, avisa y termina
        fprintf(stderr, "Índice inválido o versión incompatible\n");
        return EXIT_FAILURE;
    }

    // Leer directorio completo en RAM (~16 KB)

    // Reserva memoria para el directorio de buckets (1000 entradas típicamente)
    g_dir = (DirEntry *)malloc(sizeof(DirEntry) * g_hdr.table_size);
    // Si falla la reserva de memoria, muestra error y termina
    if (!g_dir)
    {
        perror("malloc dir");
        return EXIT_FAILURE;
    }
    // Lee desde el índice el directorio completo de buckets a memoria
    if (fread(g_dir, sizeof(DirEntry), (size_t)g_hdr.table_size, g_idx) != (size_t)g_hdr.table_size)
    {
        // Si ocurre un error al leer el directorio, muestra error y finaliza
        perror("read dir");
        return EXIT_FAILURE;
    }

    // Socket listen

    // Crea el socket TCP principal (IPv4, tipo flujo)
    int s = socket(AF_INET, SOCK_STREAM, 0);
    // Si no se puede crear el socket, muestra error y termina
    if (s < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }

    // Permite reutilizar el puerto inmediatamente tras reiniciar el servidor
    int opt = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Estructura que define la dirección IP y puerto del servidor
    struct sockaddr_in addr;
    // Inicializa la estructura a cero para evitar valores residuales
    memset(&addr, 0, sizeof(addr));
    // Define la familia de direcciones: IPv4
    addr.sin_family = AF_INET;
    // Asigna el puerto del servidor y lo convierte al orden de bytes de red
    addr.sin_port = htons((uint16_t)port);
    // Convierte la IP en texto (ej. "127.0.0.1") a formato binario; valida dirección
    if (inet_pton(AF_INET, bind_ip, &addr.sin_addr) != 1)
    {
        fprintf(stderr, "IP inválida\n");
        return EXIT_FAILURE;
    }

    // Asocia el socket a la dirección y puerto especificados (bind)
    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind");
        return EXIT_FAILURE;
    }
    // Pone el socket en modo de escucha, con cola máxima de 64 conexiones pendientes
    if (listen(s, 64) < 0)
    {
        perror("listen");
        return EXIT_FAILURE;
    }

    // Mensaje informativo: confirma IP, puerto y total de registros indexados
    fprintf(stderr, "Servidor listo en %s:%d | total=%" PRIu64 " entradas\n", bind_ip, port, g_hdr.total_entries);

    // Bucle principal: acepta clientes hasta que se reciba SIGINT (Ctrl+C)
    while (!g_stop)
    {
        // Estructura para guardar la dirección del cliente que se conecte
        struct sockaddr_in cli;
        socklen_t cl = sizeof(cli);
        // Acepta una conexión entrante y devuelve un nuevo socket para el cliente
        int cfd = accept(s, (struct sockaddr *)&cli, &cl);
        // Maneja errores al aceptar conexiones; permite salir limpiamente con Ctrl+C
        if (cfd < 0)
        {
            // Si la señal de interrupción fue recibida, sale del bucle
            if (errno == EINTR && g_stop)
                break;
            // Si ocurre otro error, muestra el error y continúa aceptando nuevas conexiones
            perror("accept");
            continue;
        }
        // Hilo por cliente (simple y suficiente)

        // Crea un nuevo hilo para manejar la conexión del cliente
        pthread_t th;
        // Reserva memoria para el contexto del cliente
        ClientCtx *ctx = (ClientCtx *)malloc(sizeof(ClientCtx));
        // Asigna el descriptor de archivo del socket del cliente al contexto
        ctx->fd = cfd;
        // Crea el hilo que ejecutará la función client_thread con el contexto del cliente
        pthread_create(&th, NULL, client_thread, ctx);
        // Desvincula el hilo para que sus recursos se liberen automáticamente al terminar
        pthread_detach(th);
    }

    // Limpieza y cierre del servidor
    close(s);
    free(g_dir);
    fclose(g_idx);
    fclose(g_csv);
    fprintf(stderr, "Servidor cerrado.\n");
    return 0;
}
