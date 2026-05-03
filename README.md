# VotaPP Pro - Simulador Big Data de votacion participativa

VotaPP Pro es un proyecto academico que simula una plataforma de votacion participativa para Medellin. El objetivo no es solamente crear registros, sino mostrar un ciclo Big Data completo: ingesta, almacenamiento transaccional, procesamiento batch con Spark, Data Lake por capas, dashboard operativo y visualizacion territorial.

El proyecto toma como referencia conceptual el sistema oficial VotaPP y adapta el ejercicio para un escenario educativo de alto volumen, usando ciudadanos, votos por proyectos, logs de operacion, comunas/corregimientos de Medellin y salidas analiticas.

## Tabla de contenido

- [Objetivo del proyecto](#objetivo-del-proyecto)
- [Arquitectura general](#arquitectura-general)
- [Servicios Docker](#servicios-docker)
- [Modelo funcional](#modelo-funcional)
- [Regla de votos y ciudadanos](#regla-de-votos-y-ciudadanos)
- [Flujo Big Data](#flujo-big-data)
- [Dashboard Streamlit](#dashboard-streamlit)
- [Mapa territorial](#mapa-territorial)
- [Data Lake](#data-lake)
- [Endpoints principales](#endpoints-principales)
- [Ejecucion del proyecto](#ejecucion-del-proyecto)
- [Entrega portable](#entrega-portable)
- [Guia de demostracion](#guia-de-demostracion)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Notas tecnicas](#notas-tecnicas)
- [Problemas comunes](#problemas-comunes)

## Objetivo del proyecto

Este ejercicio busca emular una jornada de votacion participativa en Medellin con enfoque Big Data.

El sistema permite:

- Generar ciudadanos de manera masiva.
- Simular votos por proyectos de Presupuesto Participativo.
- Simular logs de auditoria y operacion.
- Registrar datos en PostgreSQL como sistema OLTP.
- Consultar datos por API REST con FastAPI.
- Visualizar datos en Streamlit.
- Materializar un Data Lake por zonas.
- Procesar datos con Spark.
- Mostrar analitica por proyecto, comuna, canal y dependencia.
- Visualizar comunas/corregimientos en mapa interactivo.

## Arquitectura general

```text
Usuario / Dashboard Streamlit
        |
        v
FastAPI REST API
        |
        +------------------+
        |                  |
        v                  v
PostgreSQL OLTP        Kafka / eventos
        |
        v
Materializacion Big Data
        |
        v
Spark local[*]
        |
        v
Data Lake
  raw/
  bronze/
  curada/
  analytics/
        |
        v
Dashboard / BI / Mapa
```

## Servicios Docker

El proyecto se ejecuta con `docker-compose`.

| Servicio | Puerto | Funcion |
| --- | ---: | --- |
| `api` | `8000` | Backend FastAPI |
| `dashboard` | `8501` | Dashboard Streamlit |
| `db` | `5433 -> 5432` | PostgreSQL |
| `kafka` | `9092` | Broker Kafka |
| `zookeeper` | `2181` | Coordinador Kafka |
| `spark-master` | `18080`, `7077` | Spark master |
| `spark-worker` | `8081` | Spark worker |

URLs principales:

```text
API:        http://localhost:8000
Swagger:    http://localhost:8000/docs
ReDoc:      http://localhost:8000/redoc
Streamlit:  http://localhost:8501
Spark UI:   http://localhost:18080
```

## Modelo funcional

El sistema trabaja con tres grupos principales de datos.

### Ciudadanos

Representan personas registradas para participar.

Campos relevantes:

- `id_ciudadano`
- `cedula`
- `nombre`
- `email`
- `edad`
- `genero`
- `comuna`
- `barrio`
- `validado`
- `activo`
- `ha_votado`
- `comuna_votada`
- `fecha_registro`

### Votos

Representan eventos de votacion por proyecto.

Campos relevantes:

- `id_voto`
- `id_ciudadano`
- `id_proceso_electoral`
- `id_proyecto`
- `proyecto`
- `comuna`
- `dependencia`
- `eje_tematico`
- `valor_estimado`
- `orden_preferencia`
- `canal`
- `modalidad`
- `estado`
- `latencia_ms`
- `dispositivo`
- `hash_auditoria`
- `timestamp_voto`

### Logs del sistema

Representan auditoria y eventos tecnicos.

Campos relevantes:

- `id_log`
- `tipo`
- `nivel`
- `servicio`
- `evento`
- `mensaje`
- `codigo_http`
- `duracion_ms`
- `timestamp`

## Regla de votos y ciudadanos

Esta regla es importante para entender la simulacion.

En el proyecto, `total_ciudadanos` representa personas base. El limite de la simulacion aplica sobre ciudadanos:

```text
maximo ciudadanos base = 3,000,000
```

`total_votos` representa eventos de voto, no necesariamente personas unicas. Una persona puede votar por varios proyectos.

Por eso:

```text
total_votos puede ser mayor que total_ciudadanos
```

El valor que se verifica directamente contra ciudadanos es:

```text
ciudadanos_que_votaron <= total_ciudadanos
```

El campo `ha_votado` marca si una persona participo al menos una vez. Si una persona vota por varios proyectos, sigue contando como un ciudadano que voto, pero puede generar varios registros en la tabla `votos`.

Ejemplo:

```text
total_ciudadanos = 10
total_votos = 25
total_logs = 5

Resultado esperado:
ciudadanos_que_votaron = 10
votos_evento = 25
votos_por_votante_prom = 2.5
```

En la capa Silver del Data Lake los votos se deduplican por `id_voto`, no por ciudadano, para no eliminar votos validos de una misma persona hacia varios proyectos.

## Flujo Big Data

El flujo completo es:

```text
1. Simulador Streamlit
2. POST /api/v1/simulacion/generar
3. Insercion masiva por lotes en PostgreSQL
4. Consulta operativa desde FastAPI
5. Materializacion Data Lake
6. Spark transforma datos
7. Parquet por zonas
8. Dashboard y mapa consumen indicadores
```

## Dashboard Streamlit

El dashboard esta disponible en:

```text
http://localhost:8501
```

Tiene estas vistas:

| Vista | Funcion |
| --- | --- |
| `Simulador` | Genera ciudadanos, votos y logs por lotes |
| `Big Data` | Materializa Data Lake con Spark |
| `Resultados` | Muestra votos por proyecto, canal y dependencia |
| `Territorio` | Muestra ciudadanos por comuna y votos por comuna/proyecto |
| `Mapa` | Visualiza comunas/corregimientos en mapa interactivo |
| `Registros` | Muestra muestras recientes de ciudadanos y votos |
| `Auditoria` | Muestra logs y conteo de errores |

### Consola del simulador

La pestaña `Simulador` tiene una consola interactiva que emula una carga real.

Muestra:

- Payload enviado a la API.
- Barra verde de progreso.
- ETA aproximado.
- Throughput real calculado con commits confirmados.
- Chunks confirmados.
- Eventos tipo stream:
  - `CIUDADANO_REGISTRADO`
  - `IDENTIDAD_VALIDADA`
  - `VOTO_EMITIDO`
  - `LOG_SISTEMA`
  - `BRONZE_APPEND`
  - `METRICA_COMUNA`

Ejemplo:

```text
$ curl -X POST /api/v1/simulacion/generar
[payload] {"total_ciudadanos": 2990000, "total_votos": 2990000, "total_logs": 51500}
[stream] insertando chunks PostgreSQL | total=6,031,500 | lote=10,000
[status] status=running | fase=votos | procesados=4,500,000/6,031,500
[speed] throughput=16,000 reg/s | elapsed=04:41 | eta=01:35
[00577] OK LOG_SISTEMA -> votapp-logs | offset: 577
```

La API responde al final del proceso, pero durante la ejecucion publica progreso real en:

```text
GET /api/v1/simulacion/progreso/{run_id}
```

Streamlit consulta ese endpoint mientras la carga esta activa. Por eso la barra de simulacion se basa en filas realmente confirmadas por chunks y no debe devolverse.

### Consola Big Data

La pestaña `Big Data` usa una consola similar para mostrar el pipeline:

```text
postgres-export -> raw-bronze -> silver -> gold -> serving -> confirm
```

Muestra:

- Run estimado.
- Filas estimadas.
- Fase actual.
- ETA aproximado.
- Throughput.
- Zonas del Data Lake.
- Salida final real de Spark.

Ejemplo:

```text
$ curl -X POST /api/v1/bigdata/materializar-datalake
[run] 20260503_201919 | filas_estimadas~=9,064,807
[phase] gold | KPIs por proyecto, comuna, canal y dependencia
[status] procesados~=5,076,291/9,064,807 | elapsed=00:18 | eta~=00:31
[spark] master=local[*] | throughput~=275,885 filas/s

[01] OK raw      -> ciudadanos.csv     | votos.csv     | logs_sistema.csv
[02] OK bronze   -> ciudadanos.parquet | votos.parquet | logs.parquet
[03] .. silver   -> votos_normalizados | ciudadanos_dedup | logs_validos
[04] .. gold     -> votos_proyecto     | votos_comuna  | votos_dependencia
```

## Mapa territorial

La vista `Mapa` usa `pydeck` y datos geograficos de ArcGIS para pintar comunas y corregimientos de Medellin.

El mapa permite colorear por:

- Cantidad de votos por comuna.
- Participacion electoral.

La escala va de:

```text
rojo   = menor valor observado
amarillo = valor intermedio
verde  = mayor valor observado
```

El tooltip muestra:

- Comuna/corregimiento.
- Ciudadanos.
- Votos.
- Peso sobre el total.
- Participacion.
- Proyecto lider.
- Votos del proyecto lider.

La capa geografica se consulta desde ArcGIS y se transforma a `PolygonLayer` de `pydeck` para mejorar compatibilidad con Streamlit.

## Data Lake

El Data Lake se materializa desde:

```text
POST /api/v1/bigdata/materializar-datalake
```

Cada ejecucion crea un run con formato:

```text
datalake/runs/YYYYMMDD_HHMMSS/
```

Ejemplo:

```text
datalake/runs/20260503_190245/
```

### Zonas

```text
datalake/
  _latest_run.txt
  runs/
    20260503_190245/
      raw/
        ciudadanos.csv
        votos.csv
        logs_sistema.csv
      bronze/
        ciudadanos/
        votos/
        logs_sistema/
      curada/
        ciudadanos/
        votos/
        logs_sistema/
      analytics/
        votos_proyecto/
        votos_canal/
        votos_comuna/
        votos_dependencia/
        votos_comuna_proyecto/
```

### raw

Zona cruda. Contiene CSV exportados desde PostgreSQL.

### bronze

Snapshot Parquet de las tablas crudas.

### curada / silver

Datos listos para analitica:

- Ciudadanos deduplicados por `cedula`.
- Votos deduplicados por `id_voto`.
- Logs validos con `id_log`, `tipo` y `servicio`.
- Campos normalizados con `trim` y `lower`.

### analytics / gold

KPIs listos para BI:

- `votos_proyecto`
- `votos_canal`
- `votos_comuna`
- `votos_dependencia`
- `votos_comuna_proyecto`

## Endpoints principales

### Salud

```text
GET /health
```

### Simulacion

```text
POST /api/v1/simulacion/generar
GET  /api/v1/simulacion/progreso/{run_id}
```

Payload:

```json
{
  "total_ciudadanos": 1000,
  "total_votos": 700,
  "total_logs": 1500,
  "tasa_validacion": 0.75,
  "tamano_lote": 10000
}
```

### Big Data

```text
POST /api/v1/bigdata/materializar-datalake
GET  /api/v1/bigdata/datalake/estado
```

### Reportes

```text
GET /api/v1/reportes/resumen-jornada
GET /api/v1/reportes/dashboard-datos
```

### Ciudadanos

```text
POST   /api/v1/ciudadanos/
GET    /api/v1/ciudadanos/
GET    /api/v1/ciudadanos/{id_ciudadano}
PUT    /api/v1/ciudadanos/{id_ciudadano}
DELETE /api/v1/ciudadanos/{id_ciudadano}
PATCH  /api/v1/ciudadanos/{id_ciudadano}/validar
GET    /api/v1/ciudadanos/stats/por-comuna
```

### Votos

```text
POST /api/v1/votos/
GET  /api/v1/votos/
GET  /api/v1/votos/{id_voto}
GET  /api/v1/votos/stats/por-proyecto
GET  /api/v1/votos/stats/por-canal
GET  /api/v1/votos/stats/por-dependencia
GET  /api/v1/votos/stats/por-comuna-proyecto
GET  /api/v1/votos/stats/total
```

### Logs

```text
POST /api/v1/logs/
GET  /api/v1/logs/
GET  /api/v1/logs/stats/errores
```

## Ejecucion del proyecto

### Requisitos en cualquier equipo

Para ejecutar el proyecto en otro computador solo se necesita:

- Docker Desktop o Docker Engine.
- Docker Compose, preferiblemente el comando moderno `docker compose`.
- Conexion a internet para descargar imagenes Docker y la capa de mapa de ArcGIS.
- Puertos libres:
  - `8000` para FastAPI.
  - `8501` para Streamlit.
  - `5433` para PostgreSQL expuesto al host.
  - `9092` para Kafka.
  - `18080` para Spark UI.

No es necesario instalar Python localmente si se usa Docker.

### Iniciar servicios

```bash
docker compose up -d
```

### Ver estado

```bash
docker compose ps
```

### Ver logs

```bash
docker compose logs -f api
docker compose logs -f dashboard
```

### Reiniciar dashboard

```bash
docker compose restart dashboard
```

### Reiniciar API

```bash
docker compose restart api
```

### Probar API

```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/v1/reportes/dashboard-datos
```

### Abrir dashboard

```text
http://localhost:8501
```

### Ejecucion automatica

Tambien se puede usar:

```bash
chmod +x setup.sh
./setup.sh
```

El script crea `.env` desde `.env.example` si no existe, construye las imagenes y levanta los servicios.

## Entrega portable

Para enviar el proyecto no se deben incluir datos generados, caches, Data Lake ni `.env` local.

Este repositorio ya incluye:

- `.dockerignore`: evita que Docker copie archivos pesados al construir imagenes.
- `.gitignore`: evita versionar datos generados.
- `scripts/preparar_entrega.sh`: crea un ZIP limpio.

### Crear ZIP de entrega

```bash
chmod +x scripts/preparar_entrega.sh
./scripts/preparar_entrega.sh
```

Esto genera:

```text
votapp_pro_entrega.zip
```

El ZIP excluye:

- `datalake/`
- `*.csv`
- `*.parquet`
- `.env`
- `__pycache__/`
- logs
- caches de desarrollo

### Como ejecuta el receptor

Quien reciba el ZIP debe hacer:

```bash
unzip votapp_pro_entrega.zip
cd entrega_final_votapp_pro
cp .env.example .env
docker compose up -d --build
```

Luego abrir:

```text
http://localhost:8501
```

### Generar datos desde cero

El paquete se entrega sin datos masivos. El receptor debe generarlos desde la pestaña `Simulador`.

Flujo recomendado:

```text
1. Abrir Streamlit.
2. Ir a Simulador.
3. Generar una carga pequena para probar.
4. Ir a Big Data.
5. Materializar Data Lake.
6. Revisar Resultados, Territorio y Mapa.
```

Carga pequena sugerida:

```text
Ciudadanos: 1000
Votos: 1500
Logs: 3000
Tamano lote: 1000
```

Carga grande sugerida:

```text
Ciudadanos: 2990000
Votos: 2990000
Logs: 51500
Tamano lote: 10000
```

### Reiniciar datos

Si se quiere borrar la base y empezar desde cero:

```bash
docker compose down -v
docker compose up -d --build
```

Esto elimina el volumen de PostgreSQL. Usarlo solo si se desea perder los datos generados.

## Guia de demostracion

Esta secuencia sirve para presentar el proyecto.

### 1. Levantar servicios

```bash
docker compose up -d
docker compose ps
```

### 2. Abrir dashboard

```text
http://localhost:8501
```

### 3. Ejecutar simulacion

Ir a `Simulador`.

Ejemplo pequeno:

```text
Ciudadanos: 1000
Votos: 1500
Logs: 3000
Tasa validacion: 0.75
Tamano lote: 1000
```

Ejemplo grande:

```text
Ciudadanos: 2990000
Votos: 2990000
Logs: 51500
Tasa validacion: 0.85
Tamano lote: 10000
```

Explicacion para la presentacion:

```text
Los ciudadanos son personas base. Los votos son eventos. Una persona puede votar por varios proyectos, por eso votos puede ser mayor que ciudadanos.
```

### 4. Revisar resultados

Ir a:

- `Resultados`
- `Territorio`
- `Mapa`
- `Auditoria`

### 5. Materializar Data Lake

Ir a `Big Data`.

Presionar:

```text
Materializar Data Lake con Spark
```

Explicar las capas:

```text
raw -> bronze -> silver -> gold
```

### 6. Ver archivos generados

```bash
find datalake/runs -maxdepth 3 -type f | sort | tail -80
```

## Estructura del proyecto

```text
.
├── config/
│   ├── database.py
│   └── settings.py
├── dashboard/
│   ├── requirements.txt
│   └── streamlit_app.py
├── datalake/
│   ├── _latest_run.txt
│   └── runs/
├── src/
│   ├── api/
│   │   ├── bigdata.py
│   │   ├── ciudadanos.py
│   │   ├── logs.py
│   │   ├── reportes.py
│   │   ├── simulacion.py
│   │   └── votos.py
│   ├── models/
│   │   ├── ciudadano.py
│   │   ├── log_sistema.py
│   │   └── voto.py
│   ├── schemas/
│   │   └── __init__.py
│   └── services/
│       ├── data_generator.py
│       ├── database_service.py
│       ├── datalake_service.py
│       ├── kafka_producer.py
│       └── simulation_service.py
├── Dockerfile
├── Dockerfile.streamlit
├── docker-compose.yml
├── main.py
├── requirements.txt
└── README.md
```

## Notas tecnicas

### Spark

La materializacion usa:

```text
DATALAKE_SPARK_MASTER=local[*]
```

Esto permite que Spark lea y escriba archivos locales dentro del contenedor de la API sin problemas de volumen compartido entre workers.

El `docker-compose` tambien conserva `spark-master` y `spark-worker` como parte de la arquitectura del laboratorio.

### FastAPI reload

La API corre con:

```text
uvicorn main:app --host 0.0.0.0 --port 8000 --reload --reload-exclude dashboard/*
```

Se excluye `dashboard/*` para evitar que cambios en Streamlit reinicien FastAPI.

### Timeouts del dashboard

Streamlit usa:

```text
API_READ_TIMEOUT=30
```

Esto evita errores de timeout cuando la base ya tiene millones de registros y los agregados tardan mas.

### Big Data y volumen

La simulacion puede insertar millones de eventos. Con cargas grandes, la API puede tardar varios minutos. El dashboard muestra progreso real por chunks confirmados y calcula el ETA con el throughput observado. La confirmacion final depende de PostgreSQL y del tiempo de commit.

## Problemas comunes

### El dashboard muestra timeout

Causa probable:

- API reiniciandose.
- Consulta agregada sobre millones de filas.
- Job masivo todavia confirmando.

Solucion:

```bash
docker compose ps
docker compose logs --tail=100 api
docker compose restart dashboard
```

### La API no responde

Verificar:

```bash
curl http://localhost:8000/health
docker compose logs -f api
```

### El mapa no carga

Verificar conexion a internet, porque la capa geografica viene de ArcGIS.

Si ArcGIS responde lento, recargar la vista:

```text
Ctrl + F5
```

### La consola llega a 97% o 99%

Eso significa que el dashboard ya estimo casi todo el trabajo, pero la API aun no ha confirmado el final. Cuando la respuesta llega, la consola pasa a 100% y muestra el resultado real.

### Hay mas votos que ciudadanos

Es valido. Los votos son eventos por proyecto. Una persona puede votar por varios proyectos.

La validacion correcta es:

```text
ciudadanos_que_votaron <= ciudadanos_creados
```

## Resumen para exposicion

VotaPP Pro simula una jornada de votacion participativa en Medellin. Los ciudadanos, votos y logs se generan masivamente desde Streamlit y se almacenan en PostgreSQL. Luego FastAPI expone reportes operativos y Spark materializa un Data Lake por capas: raw, bronze, silver y gold. Finalmente, Streamlit permite analizar los resultados por proyecto, canal, dependencia y territorio, incluyendo un mapa interactivo de comunas y corregimientos.

La idea central del proyecto es demostrar que Big Data no es solo crear muchos registros, sino construir un flujo completo de ingesta, almacenamiento, procesamiento, gobierno por capas y visualizacion analitica.
