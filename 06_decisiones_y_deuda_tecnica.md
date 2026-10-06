# 06 — Decisiones técnicas y deuda técnica

## 1. Decisiones técnicas (formato ADR resumido)

| # | Semana | Decisión | Alternativa descartada | Por qué |
|---|---|---|---|---|
| D1 | 4 | MVP con 2 datasets (`loan_default_risk`, `personal_finance_ml`) | Usar los 3 desde el inicio | Menos alcance y un pipeline completo de punta a punta antes de crecer |
| D2 | 5 | `manifest.json` con SHA-256 versionado en Git; los datos no se versionan | Subir los CSV a Git | Linaje y detección de cambios sin inflar el repositorio |
| D3 | 6 | Parquet particionado por **fecha lógica** | CSV / partición por fecha real | Tipos y compresión; backfills reproducibles e idempotentes |
| D4 | 6 | MinIO como data lake | Disco local | API S3 real: el mismo código migra a AWS cambiando el endpoint |
| D5 | 7 | Log JSON Lines compartido por todos los DAGs | Solo logs de Airflow | Una sola fuente de eventos, fácil de leer desde el dashboard |
| D6 | 7 | `auditar_bronze` con `trigger_rule="all_done"` | Regla por defecto (`all_success`) | La auditoría debe correr justo cuando algo falló |
| D7 | 8 | Imagen propia de Airflow con dependencias instaladas | `_PIP_ADDITIONAL_REQUIREMENTS` | Arranques rápidos y reproducibles (recomendación oficial) |
| D8 | 8 | Cuarentena (`valid/` vs `quarantine/`) | Descartar registros inválidos | No se pierde información; el rechazo es auditable |
| D9 | 9 | Contrato `silver_schema.yml` → expectativas de GE generadas | Declarar reglas en código y en GE | Una sola fuente de verdad para las reglas |
| D10 | 9 | `validar_calidad` reporta pero **no bloquea** | Detener el pipeline si falla | Primera iteración del framework: evitar frenar toda la malla por una regla nueva |
| D11 | 10 | Dashboard con capa de datos separada (`data_loader.py`) | Todo en `app.py` | Se puede probar y cambiar de herramienta sin tocar la interfaz |
| D12 | 11 | Gold con **pandas**, no Spark | Otro job de Spark | ~32 K filas: Spark solo agrega latencia y complejidad |
| D13 | 11 | Regresión logística con 2 features | Modelos más complejos | 300 filas de entrenamiento: menos sobreajuste y coeficientes interpretables |
| D14 | 11 | Z-score **dentro de cada dataset** | Comparar dólares absolutos | Las fuentes no comparten escala (ahorro mensual vs. ahorro acumulado) |
| D15 | 11 | Excluir `Retirement_Age` | Mapearla a `age` | Son conceptos distintos; no se inventan equivalencias |
| D16 | 11 | Esquema estrella + validación de integridad antes de escribir | Solo tablas planas | Normalización pedida por el mentor; no publicar un modelo roto |
| D17 | 11 | Llaves surrogadas en orden alfabético | IDs aleatorios | Deterministas: la misma entrada da las mismas llaves |
| D18 | 12 | KPIs en **formato largo** con umbral y estado | Una columna por KPI | Agregar un KPI no cambia el esquema; el semáforo viaja con el dato |
| D19 | 12 | Alertas de KPI como `INFO` en el log | Registrarlas como `ALERTA` | Una alerta de negocio no es una falla del pipeline |
| D20 | 12 | Medianas para montos con cola larga | Promedios | El promedio se distorsiona por pocos valores extremos |
| D21 | 12 | Endeudamiento con pago mensual (mensualidad o amortización) | Usar `debt_to_income_ratio` | Esa columna mide deuda total (mediana 1.35), no el pago mensual |
| D22 | 12 | `SIN_DATOS` explícito para la varianza | Simular series mensuales | No presentar datos inventados como reales |

## 2. Limitaciones conocidas

| Limitación | Impacto | Mitigación / siguiente paso |
|---|---|---|
| **El modelo de riesgo solo usa monto del préstamo y ahorro** | El riesgo casi no cambia con el credit score (pérdida esperada ~41% en todas las bandas) y concentra el "riesgo alto" en préstamos grandes (94.7% de la exposición) | El credit score entra al **índice consolidado**. Siguiente paso: buscar un dataset de entrenamiento con score. |
| **Entrenamiento e inferencia en datasets distintos** | La PD es relativa (posición del cliente en su distribución), no una probabilidad calibrada | Documentado en `gold_schema.yml`. Requiere datos reales de incumplimiento de la misma cartera. |
| **El modelo se re-entrena en cada corrida** | Sin versionado de modelos | Separar entrenamiento e inferencia; registrar modelos (p. ej. MLflow). |
| **Sin historial por cliente** (una foto por cliente; las particiones diarias recargan los mismos datos) | `varianza_ingresos_gastos` en `SIN_DATOS`; no hay tendencias reales | Evaluar `ramyapintchy/personal-finance-data` (transacciones con fecha) como "cliente demo" del gemelo digital. |
| **Datos sintéticos** | 0% deficitarios, regiones perfectamente balanceadas (HHI = 0.200) | Interpretar los KPIs como demostración del pipeline, no como hallazgos de negocio. |
| **Pesos del índice consolidado por criterio experto** | No calibrados contra incumplimientos reales | Ajustables en `PESOS_INDICE_RIESGO`; calibrar cuando existan datos. |
| **LGD fija de 45%** | La pérdida esperada es una aproximación regulatoria | Reemplazar por una LGD observada. |

## 3. Deuda técnica

| Prioridad | Tema | Detalle | Acción sugerida |
|---|---|---|---|
| Alta | **`bronzeAudit` fuera de Git** | `ingestionBronzeLayer.py` importa `bronzeAudit.audit_bronze_layer`, que vive en `plugins/` (ignorada por `.gitignore`). Un clon nuevo del repo no puede cargar el DAG. | Mover el módulo a `scripts/` (o quitar `plugins/*` del `.gitignore`) y versionarlo. |
| Alta | **Credenciales en texto plano** | `admin/password123` en `docker-compose.yml` y como valores por defecto en scripts | Usar un archivo `.env` (ya ignorado) y Airflow Connections/Secrets. |
| Media | Conexiones de Airflow y buckets creados a mano | Pasos manuales para levantar el entorno | Crear conexiones con variables `AIRFLOW_CONN_*` y buckets con un servicio `minio-init`. |
| Media | Código duplicado entre DAGs | `registrar_evento` y la configuración de MinIO se repiten en varios archivos | Extraer un módulo común (p. ej. `scripts/comun.py`). |
| Media | `data/validar_conteos_silver.py` | Copia suelta en `data/`, fuera del flujo | Eliminarla o moverla a `scripts/`. |
| Baja | Fechas de prueba fijas en `validar_calidad_silver.py` | `FECHAS_PRUEBA = ["2026/09/01", "2026/09/02"]` en modo CLI | Recibir la fecha como argumento. |
| Baja | Pruebas solo para Gold | Silver y la ingesta no tienen pruebas unitarias | Agregar pruebas de los limpiadores de Spark con datos sintéticos. |
| Baja | Aviso de pandas en la Vista 3 | `groupby().apply()` sobre columnas de agrupación (`FutureWarning`) | Usar `include_groups=False` o agregar con `agg`. |
