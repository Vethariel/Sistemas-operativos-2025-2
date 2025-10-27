#define _POSIX_C_SOURCE 200809L
#define _FILE_OFFSET_BITS 64
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include <inttypes.h>
#include <errno.h>

#define TABLE_SIZE 1000
#define LINE_BUF   131072  // 128 KB

typedef struct {
    uint64_t id;
    uint64_t offset;
} Pair;

typedef struct {
    char     magic[8];          // "BKIDXv01"
    uint64_t table_size;        // 1000
    uint64_t total_entries;     // N
} Header;

typedef struct {
    uint64_t bucket_offset;     // desplazamiento en books.idx
    uint64_t bucket_count;      // nº de pares en el bucket
} DirEntry;

static inline unsigned hash_id(uint64_t id) {
    // Mezcla rápida (Knuth) y módulo 1000
    return (unsigned)((id * 2654435761UL) % TABLE_SIZE);
}

static void rstrip(char *s) {
    size_t n = strlen(s);
    while (n && (s[n-1]=='\n' || s[n-1]=='\r')) s[--n] = '\0';
}

static int cmp_pair_id(const void *a, const void *b) {
    const Pair *pa = (const Pair*)a, *pb = (const Pair*)b;
    if (pa->id < pb->id) return -1;
    if (pa->id > pb->id) return  1;
    return 0;
}

static int parse_id_first_field(const char *line, uint64_t *out_id) {
    const char *c = strchr(line, ',');
    size_t len = c ? (size_t)(c - line) : strlen(line);
    if (len == 0 || len > 32) return 0;
    char buf[40];
    memcpy(buf, line, len);
    buf[len] = '\0';

    // trim espacios/comillas
    char *s = buf;
    while (*s && (isspace((unsigned char)*s) || *s=='"')) s++;
    char *e = s + strlen(s);
    while (e > s && (isspace((unsigned char)e[-1]) || e[-1]=='"')) *--e = '\0';
    if (*s == '\0') return 0;

    // solo dígitos
    for (char *p=s; *p; ++p) if (!isdigit((unsigned char)*p)) return 0;

    errno = 0;
    uint64_t v = strtoull(s, NULL, 10);
    if (errno == ERANGE) return 0;
    *out_id = v;
    return 1;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <books_validos.csv> <books.idx>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *csv_path = argv[1];
    const char *idx_path = argv[2];

    // 1) Abrir CSV
    FILE *csv = fopen(csv_path, "r");
    if (!csv) { perror("No se pudo abrir CSV"); return EXIT_FAILURE; }

    // 2) Crear 1000 archivos temporales binarios para acumular pares
    FILE *tmp[TABLE_SIZE] = {0};
    char tmpname[64];
    for (int i=0; i<TABLE_SIZE; ++i) {
        snprintf(tmpname, sizeof(tmpname), "bucket_%03d.tmp", i);
        tmp[i] = fopen(tmpname, "wb+");
        if (!tmp[i]) { perror("No se pudo crear tmp bucket"); return EXIT_FAILURE; }
    }

    // 3) Recorrer CSV y distribuir (id, offset)
    char *line = (char*)malloc(LINE_BUF);
    if (!line) { perror("sin memoria"); return EXIT_FAILURE; }

    // Saltar y conservar header (no indexa)
    if (!fgets(line, LINE_BUF, csv)) {
        fprintf(stderr, "CSV vacío\n");
        return EXIT_FAILURE;
    }

    uint64_t total_entries = 0;
    off_t offset = 0;

    for (;;) {
        offset = ftello(csv);                  // offset al inicio de la línea
        if (!fgets(line, LINE_BUF, csv)) break;
        rstrip(line);

        if (line[0] == '\0') continue;

        uint64_t id = 0;
        if (!parse_id_first_field(line, &id)) {
            // En teoría ya está limpio; si algo raro aparece, lo saltamos.
            continue;
        }

        Pair p = { id, (uint64_t)offset };
        unsigned b = hash_id(id);
        if (fwrite(&p, sizeof(Pair), 1, tmp[b]) != 1) {
            perror("fwrite temp bucket");
            return EXIT_FAILURE;
        }
        total_entries++;
        // (Opcional) progreso: cada 1e6 líneas
        // if ((total_entries % 1000000ULL)==0) fprintf(stderr, "Progreso: %llu\n", (unsigned long long)total_entries);
    }

    free(line);
    fclose(csv);

    // 4) Preparar archivo final .idx (header + directorio)
    FILE *idx = fopen(idx_path, "wb+");
    if (!idx) { perror("No se pudo crear índice"); return EXIT_FAILURE; }

    Header hdr = {0};
    memcpy(hdr.magic, "BKIDXv01", 8);
    hdr.table_size    = TABLE_SIZE;
    hdr.total_entries = total_entries;

    if (fwrite(&hdr, sizeof(Header), 1, idx) != 1) { perror("write header"); return EXIT_FAILURE; }

    // Directorio (placeholders)
    DirEntry *dir = (DirEntry*)calloc(TABLE_SIZE, sizeof(DirEntry));
    if (!dir) { perror("sin memoria dir"); return EXIT_FAILURE; }
    long dir_pos = ftell(idx);
    if (fwrite(dir, sizeof(DirEntry), TABLE_SIZE, idx) != (size_t)TABLE_SIZE) {
        perror("write dir placeholders"); return EXIT_FAILURE;
    }

    // 5) Para cada bucket: ordenar por id y escribir bloque; registrar offset y count
    for (int i=0; i<TABLE_SIZE; ++i) {
        // tamaño en pares
        if (fflush(tmp[i]) != 0) { perror("fflush tmp"); return EXIT_FAILURE; }
        if (fseeko(tmp[i], 0, SEEK_END) != 0) { perror("seek end tmp"); return EXIT_FAILURE; }
        off_t sz = ftello(tmp[i]);
        uint64_t count = (uint64_t)(sz / (off_t)sizeof(Pair));
        dir[i].bucket_count = count;

        if (count == 0) { // bucket vacío
            // dejar offset=0, count=0
            continue;
        }

        // cargar bucket en memoria, ordenar y escribir
        if (fseeko(tmp[i], 0, SEEK_SET) != 0) { perror("seek tmp"); return EXIT_FAILURE; }
        Pair *buf = (Pair*)malloc((size_t)count * sizeof(Pair));
        if (!buf) { perror("sin memoria bucket"); return EXIT_FAILURE; }
        size_t rd = fread(buf, sizeof(Pair), (size_t)count, tmp[i]);
        if (rd != (size_t)count) { perror("fread tmp"); return EXIT_FAILURE; }

        qsort(buf, (size_t)count, sizeof(Pair), cmp_pair_id);

        dir[i].bucket_offset = (uint64_t)ftello(idx);
        if (fwrite(buf, sizeof(Pair), (size_t)count, idx) != (size_t)count) {
            perror("write bucket"); return EXIT_FAILURE;
        }
        free(buf);
    }

    // 6) Reescribir directorio con offsets reales
    if (fseeko(idx, dir_pos, SEEK_SET) != 0) { perror("seek dir"); return EXIT_FAILURE; }
    if (fwrite(dir, sizeof(DirEntry), TABLE_SIZE, idx) != (size_t)TABLE_SIZE) {
        perror("rewrite dir"); return EXIT_FAILURE;
    }
    fflush(idx);
    fclose(idx);
    free(dir);

    // 7) Cerrar y borrar temporales
    for (int i=0; i<TABLE_SIZE; ++i) {
        char name[64];
        snprintf(name, sizeof(name), "bucket_%03d.tmp", i);
        fclose(tmp[i]);
        remove(name);
    }

    fprintf(stderr,
            "OK: índice creado '%s'\n"
            "  buckets      : %d\n"
            "  total entries: %" PRIu64 "\n",
            idx_path, TABLE_SIZE, total_entries);

    return EXIT_SUCCESS;
}
