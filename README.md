# OpenAQ PM2.5 Indonesia: Data Pipeline

[![CI](https://github.com/RavaGhiffari/OpenAQDataEngineerFreeProject/actions/workflows/ci.yml/badge.svg)](https://github.com/RavaGhiffari/OpenAQDataEngineerFreeProject/actions/workflows/ci.yml)

Pipeline ELT harian yang mengambil data kualitas udara **PM2.5** dari semua stasiun pemantau di Indonesia
lewat [OpenAQ API v3](https://docs.openaq.org/), menyimpannya di **DuckDB**, mengolahnya dengan **dbt**
menjadi rata-rata harian dengan **kategori ISPU** (Permen LHK No. 14/2020), lalu menyajikannya di dashboard **Streamlit**.
Semuanya diorkestrasi oleh **Apache Airflow** di Docker.

## Arsitektur

```
 OpenAQ API v3
      │  httpx: paginasi, retry + backoff, rate limit
      ▼
 Handler/  ──────────────►  DuckDB  raw.*          (JSON asli disimpan, upsert idempotent)
                                │  dbt
                                ▼
                            staging.*              (view: rapikan tipe, nama, zona waktu)
                                │
                                ▼
                            marts.*                (table: dim_locations, dim_sensors,
                                │                   fct_pm25_hourly, fct_pm25_daily + ISPU)
                                ▼
                         dashboard/app.py (Streamlit)
```

DAG Airflow `openaq_ingest` (harian):

```
load_locations_task ─► load_hours_task ─► dbt_source_freshness_task ─► dbt_build_task
  lokasi + sensor        PM2.5 per jam       data masih baru?             run model + test
                         (window = data interval)                         (as_of_utc = data_interval_end)
```

## Stack

| Lapisan | Tool |
|---|---|
| Extract | Python 3.13, `httpx` |
| Storage | DuckDB (satu file `data/openaq.duckdb`) |
| Transform + data test | dbt-core + dbt-duckdb |
| Orkestrasi | Apache Airflow 3.1 (LocalExecutor, Postgres metadata) |
| Environment | Docker Compose |
| Serving | Streamlit |
| CI | GitHub Actions: pytest + `dbt build` |

## Model data

| Model | Grain | Isi |
|---|---|---|
| `marts.dim_locations` | 1 baris per lokasi | metadata stasiun, `is_active`, `is_within_indonesia` |
| `marts.dim_sensors` | 1 baris per sensor | parameter, satuan, lokasi |
| `marts.fct_pm25_hourly` | sensor × jam | nilai PM2.5, waktu UTC dan lokal |
| `marts.fct_pm25_daily` | lokasi × tanggal lokal | rata-rata/min/maks, `n_hours`, `is_complete_day`, `ispu_category` |

## Praktik data engineering yang diterapkan

- **Raw immutable**: respons API disimpan utuh (kolom `payload JSON`), jadi logika transformasi bisa diulang tanpa download ulang.
- **Idempotent**: `INSERT OR REPLACE` per primary key. Retry dan rerun tidak menghasilkan duplikat.
- **Windowed ingestion**: tiap run hanya mengambil `data_interval` miliknya sendiri.
- **Deterministik**: logika relatif waktu (`is_active`, pemilihan sensor aktif) memakai waktu acuan dari run, bukan `now()`.
- **Fail loudly**: `dbt source freshness` menggagalkan DAG kalau API berhenti mengirim jam baru, dan test dbt (unique, not_null, relationships, accepted_values, grain, batas 1–24 jam) menggagalkan build kalau data rusak.
- **Tahan gangguan**: retry dengan exponential backoff untuk 5xx/error jaringan, dan menunggu `x-ratelimit-reset` untuk 429.
- **Reproducible**: versi library dipatok, dbt dipasang di venv terpisah di image Airflow supaya tidak bentrok dependensi.

## Menjalankan

Siapkan API key OpenAQ di file `.env` di root:

```
OPENAQ_API_KEY=isi_key_kamu
```

**Lokal (tanpa Docker)**

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt -r dashboard/requirements.txt

python -m Handler.main               # extract + load contoh
cd openaq_dbt; dbt build; cd ..      # transform + test
streamlit run dashboard/app.py       # http://localhost:8501
```

**Airflow di Docker**

```powershell
docker compose build
docker compose up airflow-init       # sekali saja
docker compose up -d                 # UI: http://localhost:8089
```

Unpause dan trigger DAG `openaq_ingest` dari UI. Dashboard membaca file `data/openaq.duckdb` yang sama.
Selama DAG berjalan, tutup dashboard karena DuckDB hanya mengizinkan satu penulis.

**Test**

```powershell
pytest                               # unit test Handler/ (tanpa jaringan)
cd openaq_dbt; dbt source freshness; dbt build
```

## Struktur repo

```
Handler/            client.py (API), loader.py (DuckDB raw), main.py (run manual)
openaq_dbt/         proyek dbt: models/staging, models/marts, tests, macros
dags/               openaq_dag.py
dashboard/          app.py (Streamlit)
tests/              unit test pytest
scripts/            skrip eksplorasi/inspeksi (python -m scripts.<nama>)
docs/               DOKUMENTASI.md, penjelasan lengkap per file dan alasan desain
```

## Dokumentasi lengkap

[`docs/DOKUMENTASI.md`](docs/DOKUMENTASI.md) menjelaskan setiap tahap, setiap file, dan alasan di balik keputusan desainnya.
Di bagian 13 ada daftar keterbatasan dan ide pengembangan.
