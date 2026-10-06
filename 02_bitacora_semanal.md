# 02 — Bitácora semanal (Semana 4 a Semana 12)

Registro de lo que se construyó cada semana según el plan del proyecto: objetivo, entregable,
archivos, decisiones técnicas y evidencia.

> **Nota sobre los commits:** algunos mensajes de commit usan un número de semana distinto al del
> plan (por ejemplo, el commit "Semana 6" contiene la ingesta de la Semana 5). Esta bitácora sigue el
> **plan oficial**; la columna *Commit* indica dónde está el código en el historial de Git.

| Semana | Fechas | Fase | Tema | Entregable | Commit(s) |
|---|---|---|---|---|---|
| 4 | 3–7 ago | 2. Ingesta y Bronze | Exploración de datasets | Data profiling, diccionario de datos | `1758dd7` (`pathDataBBVA.py`) |
| 5 | 10–14 ago | 2. Ingesta y Bronze | Pipelines de ingesta | Pipeline de carga inicial | `1758dd7` |
| 6 | 17–21 ago | 2. Ingesta y Bronze | Implementación Bronze | Capa Bronze funcional | `1758dd7`, `6ad5242` |
| 7 | 24–28 ago | 2. Ingesta y Bronze | Automatización y monitoreo | DAG de Airflow, logs básicos | `933bc86` |
| 8 | 31 ago–4 sep | 3. Silver y calidad | Transformaciones iniciales | Jobs PySpark | `971b082` |
| 9 | 7–11 sep | 3. Silver y calidad | Validación, deduplicación, framework de calidad | Dataset Silver v1, Great Expectations | `971b082`, `55062e3` |
| 10 | 14–18 sep | 3. Silver y calidad | Dashboard de calidad | Dashboard operativo inicial | `55062e3` |
| 11 | 21–25 sep | 4. Gold y analítica | Diseño analítico y construcción Gold | Modelo analítico, tablas Gold | `162905d`, `88d9791` |
| 12 | 28 sep–2 oct | 4. Gold y analítica | KPIs financieros | KPIs calculados | `88d9791`, `9727747`, `ce3f0ef` |

---

## Semana 4 — Exploración de datasets

**Objetivo:** analizar la calidad inicial de los datos e identificar reglas de negocio.

**Qué se hizo**
- `pathDataBBVA.py` descarga tres datasets de Kaggle con `kagglehub`:
  `loan-default-risk-prediction-dataset`, `personal-finance-ml-dataset` y `personal-finance-data`.
- Genera un **reporte de profiling** por dataset con `ydata-profiling` (modo `minimal`) en `reports/*_profile.html`.
- Genera el **esqueleto del diccionario de datos** (`reports/diccionario_datos_esqueleto.csv`): tipo,
  % de nulos, valores únicos y outliers por IQR (1.5 × rango intercuartil) de cada columna, más
  campos vacíos para completar a mano: descripción, regla de negocio candidata y clasificación.

**Hallazgos que definieron reglas posteriores**
- `loan_default_risk` (300 filas) **no tiene columna identificadora** → la deduplicación debe ser por fila completa.
- `personal_finance_ml` (32,424 filas): `user_id` es único → llave de deduplicación.
- `loan_type` es nulo en ~60% de los casos, pero **solo cuando `has_loan = No`** → no es un dato faltante, es una regla de consistencia.
- `debt_to_income_ratio` tiene distribución bimodal con outliers esperados.
- Se decidió trabajar solo con **2 datasets en el MVP**; el tercero quedó en reserva.

**Tecnologías:** Python, pandas, kagglehub, ydata-profiling.

---

## Semana 5 — Construcción de pipelines de ingesta

**Objetivo:** automatizar la carga de datos.

**Qué se hizo**
- `scripts/downloadRawData.py`: descarga los 2 datasets del MVP a `data/raw/` y escribe
  `data/raw/manifest.json` con `kaggle_id`, **checksum SHA-256**, fecha de descarga y número de filas.
- Primera versión de `dags/ingestionBronzeLayer.py` (DAG `ingesta_bronze_dag`) y de `docker-compose.yml` (Airflow + Postgres).
- `.gitignore` que excluye los datos (`data/raw/*.csv`, `data/bronze/`, reportes) pero **sí versiona `manifest.json`**.

**Decisiones técnicas**
- **Manifest con SHA-256:** permite detectar si Kaggle actualizó un dataset y deja trazabilidad
  (linaje) de qué versión exacta se ingirió.
- Los datos no se versionan en Git: se regeneran con el script.

---

## Semana 6 — Implementación de la capa Bronze

**Objetivo:** persistir los datos crudos de forma confiable.

**Qué se hizo**
- El DAG convierte el CSV a **Parquet** y lo guarda particionado por fecha lógica: `<fuente>/YYYY/MM/DD/data.parquet`.
- Cada partición lleva `_ingestion_metadata.json` (fuente, kaggle_id, sha256, filas, fecha lógica, fecha real de ejecución, backend).
- Validación de **esquema de entrada**: si faltan columnas esperadas, la tarea falla con `AirflowFailException` (sin reintentos: reintentar no arregla un CSV mal formado).
- Se integró **MinIO** como data lake compatible con S3 (bucket `bronze-layer`), con backend configurable por variable de entorno (`BRONZE_BACKEND=minio|local`), usando `S3Hook` y la conexión `minio_s3_conn`.
- `scripts/bronzeHealth.py`: auditoría local que detecta particiones incompletas, JSON corruptos y **huecos de fechas** en el rango.

**Decisiones técnicas**
- **Parquet** en lugar de CSV: columnar, comprimido y conserva tipos.
- **Partición por fecha lógica** (no por fecha real): un backfill del día X siempre escribe en la carpeta X → re-ejecuciones reproducibles.
- **MinIO** reproduce localmente la API de S3: el mismo código funcionaría en AWS cambiando solo el endpoint.

---

## Semana 7 — Automatización y monitoreo inicial

**Objetivo:** programar la ejecución automática y tener visibilidad de lo que pasa.

**Qué se hizo** (versión v6 de `ingestionBronzeLayer.py`)
- DAG `@daily` con TaskFlow API; `retries=3` y `retry_delay=5 min`.
- **Logging estructurado** en JSON Lines (`data/reports/pipeline_log.jsonl`): cada tarea registra nivel, fuente, filas, duración y resultado.
- **Alertamiento:** `on_failure_callback=alertar_fallo` registra un evento `ALERTA` cuando una tarea agota sus reintentos.
- **Auditoría automática:** tarea `auditar_bronze` con `trigger_rule="all_done"`, que corre aunque falle una ingesta y valida que todas las particiones tengan sus 2 archivos.

**Evidencia:** backfills de agosto–septiembre con `auditar_bronze` reportando 60+ particiones y 0 problemas (ver `pipeline_log.jsonl`).

---

## Semana 8 — Transformaciones iniciales (Jobs PySpark)

**Objetivo:** limpieza y normalización de datos.

**Qué se hizo**
- Clúster Spark en Docker (`spark` master + `spark-worker`) y `src/spark/hello_spark.py` para
  validar la infraestructura **antes** de escribir lógica de negocio.
- `Dockerfile` propio de Airflow con Java 17 + PySpark + provider de Spark (reemplaza `_PIP_ADDITIONAL_REQUIREMENTS`).
- `src/spark/bronze_to_silver.py`: un limpiador por fuente (`CLEANERS`) que tipa columnas y aplica reglas:
  - `loan_default_risk`: `Loan_Default_Risk ∈ {0,1}`, `Retirement_Age > 0`, `Debt_Amount ≥ 0`, `Monthly_Savings ≥ 0`, sin nulos.
  - `personal_finance_ml`: `user_id` no nulo, `monthly_income_usd ≥ 0` y consistencia `has_loan`/`loan_type`.
- **Cuarentena:** los registros inválidos no se borran; van a `quarantine/` con la columna `motivo_cuarentena`.
- DAG `transformacion_silver_dag` con `SparkSubmitOperator` (driver en Airflow, executors en el clúster).

**Decisión técnica:** cuarentena en vez de descartar → no se pierde información y se puede auditar por qué se rechazó cada registro.

---

## Semana 9 — Validación, deduplicación y framework de calidad

**Objetivo:** resolver inconsistencias y definir reglas de validación declarativas.

**Qué se hizo**
- **Deduplicación** en `bronze_to_silver.py`: por `user_id` en `personal_finance_ml` y por fila completa en `loan_default_risk`, con conteo de duplicados removidos en el log.
- **Idempotencia** en `subir_silver_a_minio`: borra el prefijo antes de subir, así re-ejecutar no deja archivos viejos mezclados.
- `config/silver_schema.yml`: **contrato de esquema**, fuente única de verdad de tipos, nulabilidad, reglas y llave de deduplicación.
- `scripts/validar_calidad_silver.py`: genera **automáticamente** el Expectation Suite de Great Expectations a partir del contrato (no se declaran las reglas dos veces). Se usa desde el DAG (`validar_particion`) o por CLI.
- Tareas nuevas en el DAG: `validar_calidad` (guarda `_calidad_reporte.json`) y `congelar_silver_v1` (guarda `_silver_metadata.json`: **Dataset Silver v1**).

**Evidencia:** 8/8 expectativas cumplidas en ambas fuentes (`data/reporte_calidad_silver.json`); `loan_default_risk` pasa de 300 a 297 filas válidas.

---

## Semana 10 — Dashboard de calidad

**Objetivo:** visualizar métricas de calidad y contar la historia de los datos (*storytelling*).

**Qué se hizo**
- `dashboard/` con **Streamlit + Plotly**, en su propio contenedor (`Dockerfile.streamlit`, puerto 8501).
- `data_loader.py` (capa de datos) separado de `app.py` (interfaz).
- Vista 1 — **Salud de la malla** (desde `pipeline_log.jsonl`) y Vista 2 — **Calidad de datos** (desde `_calidad_reporte.json`, umbral 95%).
- **Resumen ejecutivo** que se arma con los datos actuales: si algo se degrada, el texto cambia solo.
- Paleta de colores fija: estados (verde/rojo), categorías (fuentes) y rampas secuenciales (bandas ordenadas).

---

## Semana 11 — Diseño analítico y construcción Gold

**Objetivo:** definir las métricas financieras y crear las tablas Gold.

**Parte 1 — Modelo de riesgo (`162905d`)**
- `scripts/modelo_riesgo.py`: `LogisticRegression` entrenada con `loan_default_risk`
  (features `Debt_Amount` y `Monthly_Savings`, target `Loan_Default_Risk`) y aplicada a `personal_finance_ml`
  mapeando `loan_amount_usd` y `savings_usd`. Estandarización **z-score dentro de cada dataset**, porque las escalas en dólares no son comparables entre fuentes.
- `Retirement_Age` se excluyó: no tiene equivalente en `personal_finance_ml`.
- Primeras tablas Gold: `gold_clientes_riesgo` y `gold_metricas_por_segmento`. Vista 3 del dashboard.

**Parte 2 — Modelo analítico normalizado (`88d9791`)** *(observación del mentor: faltaban las tablas Gold normalizadas)*
- `scripts/modelo_dimensional.py`: **esquema estrella** con 5 dimensiones (`dim_fecha`, `dim_cliente`,
  `dim_region`, `dim_banda_credito`, `dim_tipo_prestamo`) y la tabla de hechos `fact_posicion_financiera`.
- Medidas derivadas por cliente, calculadas una sola vez: flujo libre, tasa de ahorro, gasto/ingreso, meses de cobertura y pérdida esperada.
- **Validación de integridad** (11 chequeos) antes de escribir; si falla, la tarea falla.
- `config/gold_schema.yml`: diseño del modelo, bandas de credit score y definición de métricas financieras.

**Evidencia:** `_gold_metadata/<fecha>.json` con `integridad_modelo_estrella.exito_global = true`; modelo con AUC 0.86 en datos reales.

---

## Semana 12 — KPIs financieros

**Objetivo:** generar KPIs de riesgo financiero, exposición y comportamiento transaccional.

**Qué se hizo**
- `scripts/kpis_financieros.py` + tarea `calcular_kpis` en `transformacion_gold_dag`.
- **35 KPIs** en 4 familias, cada uno con unidad, descripción, umbral y estado (`OK`/`ALERTA`/`INFO`/`SIN_DATOS`):
  KPIs de negocio (10), riesgo financiero (7), exposición (10) y comportamiento transaccional (8).
- **KPIs por segmento** (región, banda de score, tipo de préstamo, edad, empleo, perfil de ahorro, nivel de riesgo).
- **KPIs de negocio de la lámina** (`ce3f0ef`):
  - Capacidad de ahorro con **intervalo de confianza 95% por bootstrap**.
  - **Endeudamiento mensual** = pago mensual / ingreso (meta < 36%), con el pago tomado de `monthly_emi_usd` o calculado con la fórmula de amortización.
  - **Índice de riesgo consolidado** 0–100 con semáforo Bajo/Medio/Alto.
  - **Varianza de ingresos/gastos**: definida, pero en `SIN_DATOS` (el dataset no trae historial mensual).
- Vista 4 del dashboard: KPIs de negocio, semáforo, gráficas y tabla por segmento.
- `tests/test_gold_kpis.py`: 17 pruebas unitarias con datos sintéticos.
- Corrección `9727747`: `FileNotFoundError` en el dashboard por la caché de listados de s3fs.

**Evidencia (partición 2026/09/22, datos reales):** AUC 0.86, exposición total $3,246 M, tasa de ahorro mediana 39.9%.
