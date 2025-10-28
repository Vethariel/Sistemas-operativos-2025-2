# ================================
# Makefile para sistema de índice binario con servidor TCP
# ================================

# Compilador y flags
CC      := gcc
CFLAGS  := -O2 -std=gnu11 -Wall -Wextra -D_FILE_OFFSET_BITS=64 -D_POSIX_C_SOURCE=200809L -pthread
LDFLAGS := -pthread

# Archivos fuente
SRC_INDEX   := build_index.c
SRC_SERVER  := idx_server.c
SRC_CLIENT  := idx_client_menu.c

# Ejecutables resultantes
BIN_INDEX   := build_index
BIN_SERVER  := idx_server
BIN_CLIENT  := idx_client_menu

# ================================
# Reglas principales
# ================================

all: $(BIN_INDEX) $(BIN_SERVER) $(BIN_CLIENT)

$(BIN_INDEX): $(SRC_INDEX)
	@echo "Compilando indexador..."
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(BIN_SERVER): $(SRC_SERVER)
	@echo "Compilando servidor..."
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(BIN_CLIENT): $(SRC_CLIENT)
	@echo "Compilando cliente..."
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# ================================
# Reglas auxiliares
# ================================

run-server:
	@echo "Ejecutando servidor en 127.0.0.1:9090..."
	./$(BIN_SERVER) 127.0.0.1 9090 books.idx books_validos.csv

run-client:
	@echo "Ejecutando cliente..."
	./$(BIN_CLIENT) 127.0.0.1 9090

index:
	@echo "Construyendo índice..."
	./$(BIN_INDEX) books_validos.csv books.idx

clean:
	@echo "Limpiando binarios y temporales..."
	rm -f $(BIN_INDEX) $(BIN_SERVER) $(BIN_CLIENT)
	rm -f bucket_*.tmp
	rm -f *.o
	rm -f books.idx

.PHONY: all clean run-server run-client index
