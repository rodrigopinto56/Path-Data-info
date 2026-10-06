# Gemelo Digital Financiero — Documentación técnica

Backend de datos del proyecto **BBVA Path Data**: un pipeline con **arquitectura Medallion**
(Bronze → Silver → Gold) que ingiere datos financieros públicos, los limpia y valida,
los modela para análisis y calcula KPIs financieros y de negocio, visibles en un dashboard.

Cubre de la **Semana 4** (inicio del trabajo con código) a la **Semana 12** (KPIs financieros).

## Contenido

| Documento | Qué responde |
|---|---|
| [01 — Arquitectura](01_arquitectura.md) | ¿Cómo está armado el sistema? Componentes, flujo de datos, almacenamiento y DAGs. |
| [02 — Bitácora semanal (S4–S12)](02_bitacora_semanal.md) | ¿Qué se construyó cada semana, con qué archivos y por qué? |
| [03 — Diccionario de datos](03_diccionario_datos.md) | ¿Qué columnas y reglas tiene cada fuente y cada tabla por capa? |
| [04 — Catálogo de KPIs](04_kpis.md) | ¿Cómo se calcula cada KPI, cuál es su meta y cómo se interpreta? |
| [05 — Operación y troubleshooting](05_operacion.md) | ¿Cómo levanto, ejecuto y pruebo el proyecto? ¿Qué hago si algo falla? |
| [06 — Decisiones técnicas y deuda técnica](06_decisiones_y_deuda_tecnica.md) | ¿Por qué se eligió cada tecnología? ¿Qué limitaciones conocidas hay? |

## Stack tecnológico

| Capa | Tecnología | Uso |
|---|---|---|
| Orquestación | Apache Airflow 2.9.3 (LocalExecutor) | DAGs diarios de ingesta, Silver y Gold |
| Almacenamiento | MinIO (compatible con S3) | Buckets `bronze-layer`, `silver-layer`, `gold-layer` |
| Procesamiento distribuido | Apache Spark (PySpark 4.0) | Limpieza Bronze → Silver |
| Procesamiento local | pandas + pyarrow | Gold, modelo estrella y KPIs |
| Calidad de datos | Great Expectations 0.18 | Expectativas generadas desde `config/silver_schema.yml` |
| Machine Learning | scikit-learn (LogisticRegression) | Modelo de riesgo de incumplimiento |
| Visualización | Streamlit + Plotly | Dashboard de calidad, riesgo y KPIs |
| Infraestructura | Docker Compose | Todos los servicios en contenedores |
| Metadatos Airflow | PostgreSQL 15 | Base de datos interna de Airflow |
| Pruebas | pytest | `tests/test_gold_kpis.py` |

## Mapa rápido del repositorio

```
PathData_backend/
├── dags/                         # Orquestación (Airflow)
│   ├── ingestionBronzeLayer.py       # S5–S7: CSV → Bronze
│   ├── transformacion_silver_dag.py  # S8–S9: Bronze → Silver + calidad
│   └── transformacion_gold_dag.py    # S11–S12: Silver → Gold + KPIs
├── src/spark/                    # Jobs de Spark
│   ├── hello_spark.py                # S8: validación del clúster
│   └── bronze_to_silver.py           # S8–S9: limpieza, cuarentena, dedup
├── scripts/                      # Lógica reutilizable (importada por los DAGs)
│   ├── downloadRawData.py            # S5: descarga Kaggle + manifest
│   ├── bronzeHealth.py               # S7: auditoría local de Bronze
│   ├── validar_calidad_silver.py     # S9: Great Expectations
│   ├── modelo_riesgo.py              # S11: modelo de riesgo
│   ├── modelo_dimensional.py         # S11: esquema estrella
│   ├── construir_gold.py             # S11: construcción de Gold
│   └── kpis_financieros.py           # S12: KPIs
├── config/                       # Contratos declarativos
│   ├── silver_schema.yml             # S9: reglas de Silver
│   └── gold_schema.yml               # S11–S12: modelo, métricas y KPIs
├── dashboard/                    # S10–S12: Streamlit
├── tests/                        # S12: pruebas unitarias
├── pathDataBBVA.py               # S4: exploración y profiling
├── docker-compose.yml / Dockerfile / Dockerfile.streamlit
└── docs/                         # Esta documentación
```

## Inicio rápido

```powershell
docker compose build
docker compose up -d
# Airflow:   http://localhost:8080  (airflow / airflow)
# MinIO:     http://localhost:9001  (admin / password123)
# Spark UI:  http://localhost:8081
# Dashboard: http://localhost:8501
```

Pasos completos (conexiones de Airflow, datos crudos, backfills) en [05 — Operación](05_operacion.md).
