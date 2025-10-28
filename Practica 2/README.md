# Sistema de Índice Hash Persistente con Comunicación Cliente–Servidor en C

## Resumen

Este proyecto implementa un sistema completo para consultar y actualizar una base de aproximadamente **1.56 millones de registros bibliográficos** almacenados en un archivo CSV. El sistema mantiene un **índice binario persistente** dividido en **1000 buckets**, combinando estructuras de datos en disco, E/S eficiente y un **servidor TCP concurrente** en C para resolver búsquedas por `Id` en tiempo casi constante.  
Además, soporta **inserciones incrementales** sin necesidad de reconstruir el índice completo.  
El cliente interactivo guía al usuario para **consultar (GET)** y **añadir (ADD)** libros, validando el formato y mostrando respuestas legibles.

### Enfoque

El diseño se basa en una división clara de responsabilidades:
- Un **indexador offline** construye el archivo `books.idx` desde el CSV limpio.
- Un **servidor** mantiene el índice y el CSV sincronizados con inserciones incrementales.
- Un **cliente** orquesta la interacción con el usuario final.

Bajo restricciones estrictas (memoria <10 MB por búsqueda, inserción <2 segundos), el sistema prioriza la lectura selectiva de buckets y la escritura “append-only” de segmentos, manteniendo coherencia en todo momento.

---

## Integrantes del equipo

**Felipe Rojas – Arquitectura y servidor**  
Diseñó el protocolo de texto (`GET/ADD/QUIT`), la gestión de hilos (`pthread`) y la lógica de inserción incremental en el índice binario. Se encargó de asegurar la consistencia y el rendimiento en disco bajo carga concurrente.

**Juan Rozo – Cliente y documentación**  
Implementó la interfaz de cliente, los mensajes de guía de campos y redactó la documentación técnica. Se centró en la experiencia del usuario y la claridad de las instrucciones.

**Daniel Gracia – Indexador y limpieza de datos**  
Desarrolló `build_index.c`, normalizó los campos y auditó el campo `Id` para garantizar unicidad y validez. Implementó el algoritmo de buckets temporales y la fusión ordenada para construir el índice binario.

---

## 1. Descripción general

El problema abordado es realizar **búsquedas eficientes** sobre un archivo CSV masivo sin cargarlo completamente en memoria y permitiendo **inserciones rápidas** sin pérdida de coherencia.  
El sistema logra esto mediante una **indexación hash persistente**, que aproxima un acceso de tiempo constante O(1): el `Id` determina el bucket (0–999), se carga solo ese bloque del índice binario, se busca el `offset` y se accede directamente a la línea en el CSV.

Para garantizar el bajo uso de memoria, el servidor conserva en RAM únicamente el **header** y el **directorio** del índice (~16 KB) y carga en cada consulta solo el bucket necesario.  
Durante una inserción, el servidor escribe la nueva línea al final del CSV y reescribe **solo el bucket afectado** al final del índice binario, actualizando su posición en el directorio.  
De esta forma, el sistema evita reindexar todo el archivo y mantiene la integridad incluso ante cortes inesperados.

---

## 2. Arquitectura del sistema

El sistema consta de tres binarios principales:

- **build_index.c** → Indexador que genera `books.idx` a partir del CSV.
- **idx_server.c** → Servidor TCP concurrente encargado de consultas y actualizaciones.
- **idx_client_menu.c** → Cliente interactivo con menú textual para enviar comandos al servidor.

El flujo de datos es el siguiente:

1. El cliente envía comandos de texto (`GET <id>` o `ADD <línea_csv>`).
2. El servidor procesa la solicitud:
   - Para `GET`, busca el `offset` del registro en el índice y devuelve los datos legibles.
   - Para `ADD`, valida duplicados, inserta la nueva línea y actualiza el bucket correspondiente.
3. El servidor responde con `OK`, `NOTFOUND` o `ERR` según el resultado.

Esta separación modular simplifica el mantenimiento y permite pruebas independientes de cada componente.

---

## 3. Lógica de hash y diseño del índice

El sistema utiliza la **función hash multiplicativa de Knuth**:

```
h(id) = (id * 2654435761) % 1000
```


Este método distribuye los identificadores de forma uniforme entre los 1000 buckets.  
Cada bucket contiene pares ordenados `(id, offset)` y su tamaño promedio es de unos pocos cientos de KB, lo que permite lecturas rápidas y predecibles.

El archivo `books.idx` se divide en tres secciones:

1. **Header**: Contiene una firma (`magic`), tamaño de tabla (`table_size=1000`) y el número total de registros (`total_entries`).
2. **Directorio**: 1000 entradas (`DirEntry`), cada una con `bucket_offset` y `bucket_count`.
3. **Datos**: Secuencia de buckets con pares ordenados `Pair {id, offset}`.

Las búsquedas se realizan en dos pasos: cálculo del bucket y búsqueda binaria dentro del bloque correspondiente.

---

## 4. Construcción del índice (build_index.c)

El indexador recorre el CSV original línea por línea.  
Por cada registro:
- Extrae el `Id` (campo 0).
- Calcula su hash y obtiene el bucket correspondiente.
- Registra el `offset` del inicio de la línea.
- Guarda temporalmente el par `(id, offset)` en un archivo intermedio `bucket_XXX.tmp`.

Una vez completada la lectura:
- Cada bucket temporal se ordena por `id`.
- Todos los buckets se concatenan en el archivo final `books.idx`.
- Se escribe el **header** y el **directorio** con los desplazamientos reales.

El resultado es un índice binario persistente, compacto y fácilmente navegable.

---

## 5. Servidor TCP: comandos y concurrencia

El servidor inicia un socket en modo escucha (por ejemplo, `127.0.0.1:9090`) y crea un hilo (`pthread`) por cada cliente conectado.  
Cada hilo procesa comandos hasta que recibe `QUIT`.

### Comandos principales:
- **GET <id>**  
  Busca el registro correspondiente y devuelve una ficha legible con los campos principales.
- **ADD <línea_csv>**  
  Valida el `Id`, inserta la línea en el CSV, actualiza el índice y confirma con `OK`.
- **QUIT**  
  Finaliza la conexión con el cliente.

El servidor mantiene abiertos los archivos `books.idx` (modo `r+b`) y `books_validos.csv` (modo `a+b`) durante toda la ejecución.  
Gracias a la arquitectura de hilos, múltiples clientes pueden realizar consultas o inserciones en paralelo sin bloquearse.

---

## 6. Cliente interactivo: guía y validación

El cliente presenta un menú simple:

```
1. Consultar libro por ID

2. Salir

3. Añadir nuevo registro
```


Para `GET`, el usuario introduce un número de ID y recibe una ficha con los principales datos del libro.  
Para `ADD`, el sistema muestra la descripción de cada campo del CSV y un ejemplo de formato, ayudando al usuario a ingresar la línea correctamente.  
El cliente construye un comando `ADD <línea_csv>` y lo envía al servidor.  
Este diseño minimiza errores de formato y simplifica las pruebas manuales.

---

## 7. Descripción de campos del CSV

Cada registro bibliográfico incluye **22 columnas** en el siguiente orden:

| Campo | Descripción |
|--------|--------------|
| **Id** | Identificador único del libro. Entero positivo. |
| **RatingDistTotal** | Conteo total de calificaciones en formato `total:X`. |
| **RatingDist5** | Número de calificaciones de 5 estrellas (`5:X`). |
| **PublishDay** | Día del mes de publicación. |
| **Name** | Título del libro. |
| **PublishMonth** | Mes de publicación (numérico). |
| **RatingDist4** | Número de calificaciones de 4 estrellas. |
| **RatingDist1** | Número de calificaciones de 1 estrella. |
| **RatingDist2** | Número de calificaciones de 2 estrellas. |
| **CountsOfReview** | Número total de reseñas. |
| **Authors** | Nombre(s) del autor o autores. |
| **RatingDist3** | Número de calificaciones de 3 estrellas. |
| **PublishYear** | Año de publicación. |
| **source_file** | Archivo CSV original del que proviene el registro. |
| **Publisher** | Editorial o sello publicador. |
| **Language** | Idioma del libro (ISO-639-1, ej. `eng`, `spa`). |
| **ISBN** | Código ISBN del libro. |
| **Description** | Descripción o sinopsis (puede contener HTML). |
| **Rating** | Promedio de calificaciones (float). |
| **pagesNumber** | Número de páginas del libro. |
| **Count of text reviews** | Número de reseñas escritas. |
| **PagesNumber** | Duplicado o corrección del campo anterior según fuente. |

El cliente muestra esta lista con ejemplos para facilitar la creación de un registro completo y válido.

---

## 8. Validaciones y rendimiento

El servidor valida al inicio que `books.idx` sea coherente (`magic` y `table_size`) y carga el directorio completo.  
Durante las operaciones, verifica:
- Que los comandos sean válidos (`GET`, `ADD`, `QUIT`).
- Que el `Id` sea numérico y no duplicado.
- Que el formato CSV cumpla la cantidad correcta de campos.

El rendimiento observado cumple con los objetivos:  
- **Búsqueda promedio:** < 0.5 s  
- **Inserción incremental:** < 2 s  
- **Memoria total:** < 10 MB  

El diseño en bloques permite que la lectura de buckets sea constante incluso con millones de registros.

---

## 9. Makefile y flujo de trabajo

El `Makefile` automatiza la compilación y ejecución del sistema con las siguientes reglas:

- `make` → Compila los tres ejecutables (`build_index`, `idx_server`, `idx_client_menu`).
- `make index` → Construye el índice binario desde el CSV limpio.
- `make run-server` → Inicia el servidor TCP.
- `make run-client` → Ejecuta el cliente interactivo.
- `make clean` → Elimina binarios y temporales.

Compila con:

```
gcc -O2 -std=gnu11 -Wall -Wextra -D_FILE_OFFSET_BITS=64 -pthread
```


El flujo típico es:

```
make
make index
make run-server
make run-client
```


---

## 10. Diseño de fallos y persistencia

El sistema es robusto frente a fallos.  
Cada inserción (`ADD`) sigue el orden:
1. Escribir nueva línea en `books_validos.csv`.
2. Escribir bucket actualizado al final de `books.idx`.
3. Actualizar directorio y header.

Este orden garantiza que el índice **nunca apunte a datos incompletos**.  
Incluso si ocurre un apagón entre pasos, el sistema se mantiene consistente.  
Opcionalmente, se podría añadir un `insert.log` con operaciones pendientes de confirmación para recuperación avanzada.

---

## 11. Licencia y reproducibilidad

El proyecto se distribuye con fines académicos bajo licencia **MIT**, que permite estudio, modificación y redistribución citando a los autores.  
Para ejecutar el sistema:
1. Verificar que `books_validos.csv` esté limpio y con encabezado correcto.
2. Ejecutar `make index` para generar el archivo `books.idx`.
3. Iniciar el servidor (`make run-server`).
4. Conectar clientes (`make run-client`).
5. Probar operaciones `GET` y `ADD`.

Este flujo reproduce completamente el comportamiento del sistema y permite medir rendimiento, coherencia y robustez bajo condiciones reales.
