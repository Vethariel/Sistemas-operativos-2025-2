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
    ClientCtx *ctx = (ClientCtx *)arg;
    int fd = ctx->fd;
    free(ctx);

    char line[256];
    for (;;)
    {
        ssize_t n = read_line(fd, line, sizeof(line));
        if (n <= 0)
            break;

        // Quita \r\n
        if (n > 0 && line[n - 1] == '\n')
            line[n - 1] = '\0';
        size_t L = strlen(line);
        if (L && line[L - 1] == '\r')
            line[L - 1] = '\0';

        if (strcasecmp(line, "QUIT") == 0)
            break;
        if (strncasecmp(line, "ADD ", 4) == 0)
        {
            // 1. Extraer línea CSV completa
            const char *csv_line = line + 4;
            while (*csv_line == ' ')
                csv_line++;

            // 2. Extraer el ID inicial
            char idbuf[32];
            const char *comma = strchr(csv_line, ',');
            if (!comma)
            {
                const char *msg = "ERR formato CSV inválido\n";
                send(fd, msg, strlen(msg), 0);
                continue;
            }
            size_t len = comma - csv_line;
            if (len >= sizeof(idbuf))
                len = sizeof(idbuf) - 1;
            memcpy(idbuf, csv_line, len);
            idbuf[len] = '\0';
            uint64_t id = strtoull(idbuf, NULL, 10);

            // 3. Verificar si el ID ya existe
            uint64_t off_exist = 0;
            int exists = find_offset(id, &off_exist);
            if (exists < 0)
            {
                const char *msg = "ERR index read error\n";
                send(fd, msg, strlen(msg), 0);
                continue;
            }
            if (exists > 0)
            {
                const char *msg = "ERR ID duplicado\n";
                send(fd, msg, strlen(msg), 0);
                continue;
            }

            // 4. Escribir la nueva línea al final del CSV
            fseeko(g_csv, 0, SEEK_END);
            uint64_t offset = (uint64_t)ftello(g_csv);
            fprintf(g_csv, "%s\n", csv_line);
            fflush(g_csv);

            // 5. Insertar en el índice binario
            if (insert_into_index(id, offset) != 0)
            {
                const char *msg = "ERR inserción en índice\n";
                send(fd, msg, strlen(msg), 0);
                continue;
            }

            // 6. Confirmar al cliente
            const char *okmsg = "OK Registro agregado correctamente\n";
            send(fd, okmsg, strlen(okmsg), 0);
            continue;
        }
        else if (strncasecmp(line, "GET ", 4) != 0)
        {
            const char *msg = "ERR expected: GET <id> or ADD <csv>\n";
            send(fd, msg, strlen(msg), 0);
            send(fd, "\n", 1, 0);
            continue;
        }

        char *p = line + 4;
        while (*p == ' ')
            p++;
        if (*p == '\0')
        {
            const char *msg = "ERR missing id\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        // parse id
        errno = 0;
        char *endp = NULL;
        uint64_t id = strtoull(p, &endp, 10);
        if (errno == ERANGE || endp == p)
        {
            const char *msg = "ERR bad id\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        uint64_t off = 0;
        int r = find_offset(id, &off);
        if (r < 0)
        {
            const char *msg = "ERR internal\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }
        if (r == 0)
        {
            const char *msg = "NOTFOUND\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        char *csv_line = NULL;
        size_t csv_len = 0;
        if (read_csv_line_at(off, &csv_line, &csv_len) != 0)
        {
            const char *msg = "ERR readcsv\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        // Respuesta: "OK <nbytes>\n<linea>"
        // Generar ficha formateada
        char *ficha = format_record(csv_line);
        free(csv_line);

        if (!ficha)
        {
            const char *msg = "ERR format\n";
            send(fd, msg, strlen(msg), 0);
            continue;
        }

        // Enviar ficha legible
        send(fd, ficha, strlen(ficha), 0);
        free(ficha);
    }

    close(fd);
    return NULL;
}

// ====== Main: servidor TCP ======
int main(int argc, char **argv)
{
    if (argc < 5)
    {
        fprintf(stderr, "Uso: %s <bind_ip> <port> <books.idx> <books_validos.csv>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *bind_ip = argv[1];
    int port = atoi(argv[2]);
    const char *idx_path = argv[3];
    const char *csv_path = argv[4];

    signal(SIGINT, handle_sigint);

    // Abrir índice y CSV
    g_idx = fopen(idx_path, "r+b"); // lectura/escritura binaria
    if (!g_idx)
    {
        perror("open idx");
        return EXIT_FAILURE;
    }

    g_csv = fopen(csv_path, "a+b"); // append + lectura
    if (!g_csv)
    {
        perror("open csv");
        return EXIT_FAILURE;
    }

    // Leer header
    if (fread(&g_hdr, sizeof(g_hdr), 1, g_idx) != 1)
    {
        perror("read header");
        return EXIT_FAILURE;
    }
    if (memcmp(g_hdr.magic, "BKIDXv01", 8) != 0 || g_hdr.table_size != 1000)
    {
        fprintf(stderr, "Índice inválido o versión incompatible\n");
        return EXIT_FAILURE;
    }

    // Leer directorio completo en RAM (~16 KB)
    g_dir = (DirEntry *)malloc(sizeof(DirEntry) * g_hdr.table_size);
    if (!g_dir)
    {
        perror("malloc dir");
        return EXIT_FAILURE;
    }
    if (fread(g_dir, sizeof(DirEntry), (size_t)g_hdr.table_size, g_idx) != (size_t)g_hdr.table_size)
    {
        perror("read dir");
        return EXIT_FAILURE;
    }

    // Socket listen
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }

    int opt = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, bind_ip, &addr.sin_addr) != 1)
    {
        fprintf(stderr, "IP inválida\n");
        return EXIT_FAILURE;
    }

    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind");
        return EXIT_FAILURE;
    }
    if (listen(s, 64) < 0)
    {
        perror("listen");
        return EXIT_FAILURE;
    }

    fprintf(stderr, "Servidor listo en %s:%d | total=%" PRIu64 " entradas\n", bind_ip, port, g_hdr.total_entries);

    while (!g_stop)
    {
        struct sockaddr_in cli;
        socklen_t cl = sizeof(cli);
        int cfd = accept(s, (struct sockaddr *)&cli, &cl);
        if (cfd < 0)
        {
            if (errno == EINTR && g_stop)
                break;
            perror("accept");
            continue;
        }
        // Hilo por cliente (simple y suficiente)
        pthread_t th;
        ClientCtx *ctx = (ClientCtx *)malloc(sizeof(ClientCtx));
        ctx->fd = cfd;
        pthread_create(&th, NULL, client_thread, ctx);
        pthread_detach(th);
    }

    close(s);
    free(g_dir);
    fclose(g_idx);
    fclose(g_csv);
    fprintf(stderr, "Servidor cerrado.\n");
    return 0;
}
