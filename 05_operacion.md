# 05 — Operación y troubleshooting

## 1. Requisitos

- Docker Desktop con Docker Compose.
- Python 3.11+ en el host, solo para descargar datos y correr pruebas.
- Cuenta de Kaggle configurada para `kagglehub`.

## 2. Levantar el entorno desde cero

```powershell
# 1. Datos crudos (genera data/raw/*.csv y actualiza manifest.json)
pip install kagglehub
python scripts/downloadRawData.py

# 2. Construir imágenes e inicializar Airflow
docker compose build
docker compose up airflow-init
docker compose up -d
```

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO (consola) | http://localhost:9001 | admin / password123 |
| Spark UI | http://localhost:8081 | — |
| Dashboard | http://localhost:8501 | — |

### 2.1 Configuración manual (una sola vez)

Estas piezas **no están versionadas** y se crean a mano:

1. **Buckets en MinIO**: `bronze-layer` y `silver-layer` (consola de MinIO → *Create bucket*).
   `gold-layer` se crea solo la primera vez que corre `construir_gold`.
2. **Conexiones en Airflow** (*Admin → Connections*):

   | Conn Id | Tipo | Configuración |
   |---|---|---|
   | `minio_s3_conn` | Amazon Web Services | Access key `admin`, secret `password123`, Extra: `{"endpoint_url": "http://minio:9000"}` |
   | `spark_default` | Spark | Host `spark://spark`, puerto `7077` |

3. **Módulo de auditoría de Bronze**: `ingestionBronzeLayer.py` importa `bronzeAudit`, que vive en
   `plugins/` (carpeta excluida por `.gitignore`). Ver [06 — Deuda técnica](06_decisiones_y_deuda_tecnica.md).

## 3. Ejecutar el pipeline

Orden: **Bronze → Silver → Gold**. Cada DAG procesa la partición de su fecha lógica.

### 3.1 Backfill de una fecha específica

Los DAGs son `@daily` en zona `America/Mexico_City` (UTC−6): cada corrida inicia a las **00:00 de México = 06:00 UTC**.
Por eso la fecha debe llevar la zona horaria explícita:

```powershell
$F = "2026-09-22T00:00:00-06:00"
docker compose exec airflow-scheduler airflow dags backfill ingesta_bronze_dag        -s $F -e $F --reset-dagruns
docker compose exec airflow-scheduler airflow dags backfill transformacion_silver_dag -s $F -e $F --reset-dagruns
docker compose exec airflow-scheduler airflow dags backfill transformacion_gold_dag   -s $F -e $F --reset-dagruns
```

Para ver el resultado de cada tarea en la terminal (sin pasar por el scheduler):

```powershell
docker compose exec airflow-scheduler airflow dags test transformacion_gold_dag "2026-09-22T00:00:00-06:00"
```

### 3.2 Ejecutar Gold y KPIs a mano (depuración)

```powershell
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/scripts && python construir_gold.py 2026/09/22 && python kpis_financieros.py 2026/09/22"
```

`construir_gold.py` debe correr primero: escribe las tablas `dim_*` y `fact_*` que lee `kpis_financieros.py`.

### 3.3 Validaciones manuales

```powershell
# Calidad de Silver (Great Expectations, modo CLI)
docker compose exec airflow-scheduler python /opt/airflow/scripts/validar_calidad_silver.py
# Salud de Bronze en disco local (backend=local)
python scripts/bronzeHealth.py
```

## 4. Pruebas

```powershell
pip install pandas pyarrow scikit-learn pytest
pytest tests/
```

`tests/test_gold_kpis.py` (17 pruebas, datos sintéticos, no necesita Docker) cubre:
- **Modelo estrella**: tablas completas, integridad referencial, detección de FK huérfanas, grano, miembro "Sin prestamo", normalización y pérdida esperada.
- **KPIs**: catálogo completo, cuadre de la exposición con la tabla de hechos, semáforo de umbrales y cuadre de segmentos contra el total.
- **KPIs de negocio**: fuente del pago mensual (mensualidad real, amortización o N/D), índice consolidado en sus extremos, intervalo de confianza y suma del semáforo = 100%.

