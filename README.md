# N-BeMod — Behavioural Model Platform

> **Calibración y modelización de modelos de comportamiento de clientes**  
> ALM · IRRBB · Liquidity · FP&A — MVP v0.1.0

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                         N-BeMod Stack                           │
├──────────┬──────────┬──────────┬──────────────┬────────────────┤
│ Streamlit│ FastAPI  │  Celery  │  PostgreSQL  │   MinIO S3     │
│   UI     │   API    │ Workers  │  Metadata +  │  Raw / Curated │
│  :8501   │  :8000   │          │  Versioning  │  Results       │
│          │          │          │  Audit       │  :9000 / :9001 │
├──────────┴──────────┴──────────┴──────────────┴────────────────┤
│                          Redis :6379                            │
│                  Broker + Result Backend                        │
└─────────────────────────────────────────────────────────────────┘
```

### Flujo de datos (vertical slice)
```
Upload CSV/Excel
    ↓
[MinIO] raw/
    ↓
[Worker] normalize → curated/ (Parquet)
    ↓
[Worker] DQ checks → dq_report.json → status OK/WARN/KO
    ↓
[API] POST /models/prepay_curve/calibrate
    ↓
[Worker] calibrate_simple_average → curves.parquet + params.json
    ↓
[API] POST /runs {model_version_id, scenario}
    ↓
[Worker] compute_cashflows → cashflows.parquet + export.xlsx
    ↓
[API] GET /artifacts/{id}/download → Excel
```

---

## Arranque rápido

### 1. Clonar y configurar

```bash
git clone <repo-url> nbemod
cd nbemod
cp .env.example .env
```

### 2. Levantar servicios

```bash
docker compose up --build -d
```

Servicios disponibles:
| Servicio     | URL                        |
|--------------|----------------------------|
| UI           | http://localhost:8501      |
| API (docs)   | http://localhost:8000/docs |
| MinIO Console| http://localhost:9001      |

### 3. Ejecutar migraciones DB

```bash
docker compose exec api alembic upgrade head
```

### 4. Generar dataset demo

```bash
docker compose exec api python -m scripts.generate_demo_data
# → data/demo_loans.csv (400 filas, 3 portfolios, 5 segmentos)
```

### 5. Workflow completo (smoke test)

```bash
# Crear entidad
curl -X POST http://localhost:8000/entities \
  -H "Content-Type: application/json" \
  -d '{"name": "Banco Demo", "description": "Entidad de prueba"}'

# Upload dataset
curl -X POST http://localhost:8000/datasets/loans/upload \
  -F "file=@data/demo_loans.csv" \
  -F "entity_id=<entity_id>" \
  -F "as_of_date=2024-12-31"

# Esperar DQ (status OK/WARN)
curl http://localhost:8000/datasets/<dataset_version_id>

# Calibrar
curl -X POST http://localhost:8000/models/prepay_curve/calibrate \
  -H "Content-Type: application/json" \
  -d '{"dataset_version_id": "<id>", "horizon_months": 60}'

# Ejecutar run
curl -X POST http://localhost:8000/runs \
  -H "Content-Type: application/json" \
  -d '{"model_version_id": "<id>", "scenario_name": "Base"}'

# Descargar Excel
curl -o export.xlsx http://localhost:8000/runs/artifacts/<artifact_id>/download
```

---

## Estructura del repositorio

```
nbemod/
├── api/
│   ├── main.py              # FastAPI app
│   ├── routers/
│   │   ├── health.py
│   │   ├── entities.py
│   │   ├── datasets.py
│   │   ├── models.py
│   │   └── runs.py
│   └── Dockerfile
├── worker/
│   ├── tasks.py             # Celery tasks
│   └── Dockerfile
├── db/
│   ├── models.py            # SQLAlchemy ORM
│   ├── session.py           # DB session management
│   └── migrations/          # Alembic
├── models/
│   ├── dq.py                # Data Quality checks
│   ├── prepay_curve.py      # CPR/SMM calibration
│   ├── cashflows.py         # Cashflow engine
│   └── export.py            # Excel builder
├── storage/
│   └── minio_client.py      # MinIO client + path conventions
├── ui/
│   ├── app.py               # Streamlit UI
│   └── Dockerfile
├── scripts/
│   └── generate_demo_data.py
├── docs/
├── docker-compose.yml
├── .env.example
├── requirements.txt
└── README.md
```

---

## Modelos implementados (MVP)

### `prepay_curve` — Simple Average CPR/SMM

| Parámetro       | Descripción                              | Default |
|-----------------|------------------------------------------|---------|
| `curve_method`  | `simple_average` (cohort en P1)          | simple_average |
| `horizon_months`| Horizonte de proyección en meses         | 60 |
| `min_segment_size` | Mínimo de contratos por segmento      | 10 |
| `smoothing`     | Suavizado rolling average (3 períodos)   | false |

**Lógica de calibración:**
- CPR base = f(avg_rate por segmento) — heurística en MVP; reemplazable por regresión con datos históricos
- Seasoning ramp estilo PSA (PSA-inspired): CPR crece linealmente hasta el mes 30
- SMM = 1 - (1 - CPR)^(1/12)
- Output: tabla `[segment, month, cpr, smm]`

**Cashflow engine:**
```
opening_balance[t] = closing_balance[t-1]
prepayment[t]      = smm[t] × opening_balance[t]
closing_balance[t] = opening_balance[t] - prepayment[t]
```

---

## Data Quality (v0) — 20 checks

| Check | Resultado |
|-------|-----------|
| Campos requeridos no nulos | OK / WARN / KO |
| Balance > 0 y numérico | OK / WARN / KO |
| Rate en rango [-5%, 50%] | OK / WARN |
| Sin duplicados (contract_id o clave alternativa) | OK / WARN / KO |
| Fechas parseables | OK / WARN |
| % nulos por columna | OK / WARN / KO |
| Mínimo de filas | OK / WARN / KO |
| Columna segment presente | OK / WARN |
| Balance numérico | OK / KO |
| Outliers balance (IQR ×3) | OK / WARN |

---

## Versionado y trazabilidad

Cada resultado es **completamente trazable**:
```
ResultArtifact → ScenarioRun → ModelVersion → DatasetVersion → Entity
                              ↓                ↓
                         params_json        file_hash + raw_path
                         curves_path        curated_path
                                            dq_report_path
```

---

## Roadmap (P1)

| Feature | Descripción |
|---------|-------------|
| Cohort CPR | Curvas por vintage/cohorte |
| Backtesting v0 | Validación rolling WAPE/MAPE |
| Survival models | Weibull, Cox PH para prepagos |
| NMD models | Modelos de vencimientos para cuentas sin vencimiento |
| Early cancellation | Tasa de precancelación de depósitos a plazo |
| IRRBB integration | Inputs downstream: EVE, NII |
| Auth/RBAC | Usuarios, roles, audit log |
| Monte Carlo | Simulación estocástica de escenarios |
| Next.js UI | Migración UI a stack React (backend sin cambios) |

---

## Cumplimiento normativo (EBA / BIS)

La arquitectura está diseñada para cumplir:
- **EBA GL/2022/14** (IRRBB): modelos de comportamiento auditables, versionados y con backtesting
- **BIS Basel III** (IRRBB Pillar 2): separación entre supuestos de comportamiento y escenarios de tasas
- **Principios de trazabilidad**: cada run registra dataset_version, model_version, params y timestamps
- **Versionado de modelos**: modelo de gobernanza con ModelDefinition → ModelVersion → ScenarioRun

---

*N-BeMod © 2024 — MVP v0.1.0*
