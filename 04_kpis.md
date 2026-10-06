# 04 — Catálogo de KPIs

Implementación: `scripts/kpis_financieros.py` (`CATALOGO_KPIS`). Definición formal: `config/gold_schema.yml`
(`parametros_financieros`, `metricas_financieras`, `indice_riesgo_consolidado`).
Salida: `gold_kpis_financieros` (global) y `gold_kpis_por_segmento`.

## Estados

| Estado | Significado |
|---|---|
| `OK` | Tiene umbral y está del lado sano |
| `ALERTA` | Tiene umbral y lo rebasa → requiere atención |
| `INFO` | Informativo, sin umbral |
| `SIN_DATOS` | No se puede calcular con los datos actuales (se muestra "N/D", nunca 0) |

## Parámetros de negocio

| Parámetro | Valor | Justificación |
|---|---|---|
| LGD (pérdida si incumple) | 45% | Valor supervisor de Basilea II (IRB básico) para deuda sin garantía |
| Meta de endeudamiento mensual | < 36% | Lámina "KPI Negocio" |
| Meta de capacidad de ahorro | ≥ 20% | Lámina "KPI Negocio" (regla 50/30/20) |
| Colchón mínimo | 3 meses de gasto | Fondo de emergencia estándar |
| Riesgo alto (modelo) | PD ≥ 0.5 | Umbral de clasificación |
| Bootstrap | 500 re-muestras, semilla 42 | Intervalo de confianza reproducible |

## 1. KPIs de negocio (lámina "KPI Negocio")

| KPI | Fórmula | Meta / alerta |
|---|---|---|
| `capacidad_ahorro_mediana_pct` | mediana((ingreso − gasto) / ingreso) × 100 | ALERTA si < 20% |
| `capacidad_ahorro_ic95_inf_pct` / `_sup_pct` | Percentiles 2.5 y 97.5 de 500 medianas bootstrap | Informativo ("bandas de confianza") |
| `endeudamiento_mensual_mediana_pct` | mediana(pago mensual / ingreso) × 100, clientes con préstamo | ALERTA si > 36% |
| `pct_clientes_endeudamiento_alto` | % de clientes con préstamo y endeudamiento > 36% | ALERTA si > 25% |
| `varianza_ingresos_gastos` | Coeficiente de variación a 3–6 meses | `SIN_DATOS` (no hay historial mensual) |
| `indice_riesgo_consolidado_promedio` | promedio del índice 0–100 | ALERTA si > 40 |
| `pct_clientes_semaforo_bajo` / `_medio` / `_alto` | % de clientes por nivel del índice | ALERTA si rojo > 20% |

### Pago mensual de deuda (para el endeudamiento)

Se usa la mejor fuente disponible, y queda registrada en `_gold_metadata.fuente_pago_mensual`:

1. `monthly_emi_usd` (mensualidad real), si existe.
2. Fórmula de amortización con `loan_amount_usd` (P), `loan_interest_rate_pct` (tasa anual) y `loan_term_months` (n):
   `pago = P · r / (1 − (1 + r)^−n)`, con `r = tasa anual / 12`; si la tasa es 0, `pago = P / n`.
3. Si no hay ninguna: nulo → KPI en `SIN_DATOS`.

Clientes sin préstamo: pago = 0.

### Índice de riesgo financiero consolidado (0 = sano, 100 = riesgoso)

Cada componente se lleva a una escala 0–1 de forma lineal entre un valor "sano" y uno "riesgoso" (recortado a [0, 1]) y se promedia con pesos:

| Componente | Peso | Sano (0) | Riesgoso (1) |
|---|---|---|---|
| Endeudamiento mensual | 30% | ≤ 20% | ≥ 36% |
| Probabilidad de incumplimiento (modelo) | 30% | 0 | 1 |
| Capacidad de ahorro | 20% | ≥ 20% | ≤ 0% |
| Credit score | 20% | ≥ 740 | ≤ 580 |

- **Semáforo:** Bajo < 40 · Medio 40–69.9 · Alto ≥ 70.
- **Dato faltante:** el peso de ese componente se reparte entre los demás (no cuenta como 0, para no "premiar" al cliente).
- **Pesos:** criterio experto; se ajustan en `PESOS_INDICE_RIESGO` y `ESCALAS_INDICE_RIESGO` (`scripts/modelo_dimensional.py`).

## 2. Riesgo financiero

| KPI | Fórmula | Alerta |
|---|---|---|
| `tasa_incumplimiento_historica_pct` | mean(`Loan_Default_Risk`) × 100 en Silver | — |
| `probabilidad_incumplimiento_promedio_pct` | mean(`riesgo_score`) × 100 | > 30% |
| `pct_clientes_riesgo_alto` | % con PD ≥ 0.5 | > 20% |
| `credit_score_promedio` | mean(`credit_score`) | < 670 |
| `pct_clientes_subprime` | % con score < 580 | > 20% |
| `dti_mediana` | mediana(`debt_to_income_ratio`) con préstamo | — (deuda total vs. ingreso, informativo) |
| `auc_modelo_riesgo` | AUC del modelo en el set de prueba | < 0.70 |

## 3. Exposición

| KPI | Fórmula | Alerta |
|---|---|---|
| `num_prestamos_activos` | Σ `tiene_prestamo` | — |
| `pct_clientes_con_prestamo` | % con préstamo | — |
| `exposicion_total_usd` | Σ `loan_amount_usd` (EAD) | — |
| `exposicion_promedio_usd` | exposición total / préstamos | — |
| `exposicion_riesgo_alto_usd` | Σ monto de clientes con PD ≥ 0.5 | — |
| `pct_exposicion_riesgo_alto` | exposición riesgo alto / total × 100 | > 20% |
| `perdida_esperada_usd` | Σ (PD × EAD × LGD) | — |
| `perdida_esperada_pct_exposicion` | pérdida esperada / exposición × 100 | > 5% |
| `indice_concentracion_region_hhi` | Σ (participación de cada región)² | > 0.25 (concentrada) |
| `pct_exposicion_region_principal` | exposición de la región mayor / total | — |

## 4. Comportamiento transaccional

| KPI | Fórmula | Alerta |
|---|---|---|
| `ingreso_mensual_promedio_usd` | mean(ingreso) | — |
| `gasto_mensual_promedio_usd` | mean(gasto) | — |
| `ratio_gasto_ingreso_mediana` | mediana(gasto / ingreso) | — |
| `tasa_ahorro_mediana_pct` | mediana(tasa de ahorro) × 100 | < 20% |
| `flujo_libre_mensual_mediana_usd` | mediana(ingreso − gasto) | — |
| `pct_clientes_deficitarios` | % con gasto > ingreso | > 15% |
| `meses_cobertura_ahorro_mediana` | mediana(ahorro / gasto) | < 3 |
| `pct_clientes_sin_colchon` | % con cobertura < 3 meses | > 30% |

Para montos con cola larga (ahorro, flujo libre, cobertura) se usa la **mediana**: el promedio se distorsiona por pocos clientes con valores extremos.

## 5. Segmentación

`gold_kpis_por_segmento` abre los KPIs clave por: región, banda de credit score, tipo de préstamo, rango de edad,
situación laboral, perfil de ahorro (Deficitario < 0% · Equilibrado 0–20% · Ahorrador ≥ 20%) y nivel del índice consolidado.

## 6. Resultados de referencia (partición 2026/09/22, datos reales)

| KPI | Valor | Lectura |
|---|---|---|
| Tasa de ahorro mediana | 39.9% | Arriba de la meta de 20% |
| Clientes deficitarios | 0.0% | Ningún cliente gasta más de lo que gana (típico de datos sintéticos) |
| AUC del modelo | 0.86 | Buen poder discriminante sobre `loan_default_risk` |
| Exposición total | $3,246 M | 12,995 préstamos (40.1% de los clientes) |
| % exposición en riesgo alto | 94.7% | El modelo asocia montos grandes con riesgo alto (ver limitaciones en [06](06_decisiones_y_deuda_tecnica.md)) |
| HHI por región | 0.200 | Reparto igual entre 5 regiones (sin concentración) |

*Los KPIs de negocio (endeudamiento mensual, índice consolidado) se agregaron después de esta corrida;
sus valores reales se obtienen al volver a ejecutar `transformacion_gold_dag`.*
