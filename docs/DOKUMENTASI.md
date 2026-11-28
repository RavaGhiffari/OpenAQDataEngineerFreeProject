# Dokumentasi Proyek OpenAQ PM2.5 Indonesia

Dokumen ini menjelaskan isi repo secara runut, mulai dari data diambil dari API sampai menjadi tabel siap pakai dan dashboard. Isinya juga mencakup **cara berpikir seorang data engineer** di balik setiap keputusan.

> Cara membaca: kalau baru mulai, baca **Bagian 1–3** dulu (gambaran besar). Bagian 4 dan seterusnya adalah penjelasan per file, dan bisa dibaca sambil membuka kodenya.

---

## Daftar Isi

1. [Ringkasan proyek](#1-ringkasan-proyek)
2. [Cara berpikir data engineer (garis besar)](#2-cara-berpikir-data-engineer-garis-besar)
3. [Peta repo dan alur end-to-end](#3-peta-repo-dan-alur-end-to-end)
4. [Tahap 0: Eksplorasi API](#4-tahap-0--eksplorasi-api)
5. [Tahap 1: Extract (`Handler/client.py`)](#5-tahap-1--extract-handlerclientpy)
6. [Tahap 2: Load ke RAW (`Handler/loader.py`)](#6-tahap-2--load-ke-raw-handlerloaderpy)
7. [Menjalankan Extract dan Load secara manual (`Handler/main.py`)](#7-menjalankan-extract--load-manual-handlermainpy)
8. [Tahap 3: Transform dengan dbt (`openaq_dbt/`)](#8-tahap-3--transform-dengan-dbt-openaq_dbt)
9. [Tahap 4: Orkestrasi dengan Airflow (`dags/openaq_dag.py`)](#9-tahap-4--orkestrasi-dengan-airflow-dagsopenaq_dagpy)
10. [Tahap 5: Environment dengan Docker](#10-tahap-5--environment-dengan-docker)
11. [Tahap 6: Serving / Dashboard (`dashboard/app.py`)](#11-tahap-6--serving--dashboard-dashboardapppy)
12. [Cara menjalankan](#12-cara-menjalankan)
13. [Keterbatasan dan ide pengembangan](#13-keterbatasan--ide-pengembangan)
14. [Glosarium](#14-glosarium)

---

## 1. Ringkasan proyek

**Tujuan:** setiap hari, secara otomatis:

1. mengambil data kualitas udara (**PM2.5**) dari seluruh stasiun pemantau di **Indonesia** lewat **OpenAQ API v3**,
2. menyimpan data mentahnya ke database **DuckDB**,
3. mengolahnya dengan **dbt** menjadi tabel analitik, termasuk **kategori ISPU** harian (Baik / Sedang / Tidak Sehat / Sangat Tidak Sehat / Berbahaya),
4. menampilkannya di **dashboard Streamlit**.

**Teknologi dan perannya:**

| Teknologi | Peran | Analogi dapur |
|---|---|---|
| OpenAQ API | Sumber data | Pasar |
| Python + `httpx` | Mengambil data (Extract) | Belanja |
| DuckDB | Database penyimpanan (satu file `.duckdb`) | Kulkas dan gudang |
| dbt | Transformasi SQL + test kualitas data | Memasak + mencicipi |
| Apache Airflow | Penjadwal dan pengatur urutan (orkestrasi) | Kepala dapur |
| Docker | Lingkungan yang seragam dan terisolasi | Gedung dapur |
| Streamlit | Menyajikan hasil (serving) | Pelayan |

Pola yang dipakai adalah **ELT (Extract → Load → Transform)**: data disimpan mentah dulu, baru diolah di dalam database.

---

## 2. Cara berpikir data engineer (garis besar)

Seorang data engineer biasanya tidak mulai dari tool, tapi dari **pertanyaan**. Urutan berpikir di proyek ini:

### 2.1 Mulai dari pertanyaan bisnis
> "Bagaimana kualitas udara (PM2.5) harian di tiap lokasi di Indonesia, dan masuk kategori ISPU apa?"

Dari pertanyaan ini kita tahu **output akhir** yang dibutuhkan: tabel *satu baris per lokasi per hari* berisi rata-rata PM2.5 dan kategori ISPU. Tabel inilah `marts.fct_pm25_daily`.

### 2.2 Kenali sumber data
- Di mana datanya? OpenAQ API v3.
- Bagaimana strukturnya? Ada **location** (stasiun) yang punya banyak **sensor**, dan tiap sensor mengukur satu parameter (pm25, pm10, o3, …). Tiap sensor punya **measurement** per jam.
- Apa batasannya? Butuh API key, ada **rate limit**, hasil **dipaginasi** (maksimal 1000 baris per halaman).
- Bagaimana kualitasnya? Ada nilai kosong, ada lokasi yang salah tag negara, dan waktunya dalam UTC.

Tahap ini dilakukan di `scripts/explore_api.py`. **Selalu eksplor dulu sebelum menulis pipeline.**

### 2.3 Prinsip-prinsip yang dipakai

| Prinsip | Artinya | Di mana di proyek ini |
|---|---|---|
| **Raw itu suci (immutable)** | Simpan data persis seperti dari sumber, termasuk JSON aslinya. Kalau logika olahan salah, cukup olah ulang tanpa download lagi. | Kolom `payload JSON` di tabel `raw.*` |
| **Idempotent** | Menjalankan pipeline 2x dengan input sama memberi hasil sama, tanpa duplikat. | `INSERT OR REPLACE` + `PRIMARY KEY` di `loader.py` |
| **Berlapis (layering)** | raw → staging → marts. Tiap lapis punya satu tanggung jawab. | `openaq_dbt/models/staging`, `marts` |
| **Tentukan grain** | "Satu baris mewakili apa?" Ini pertanyaan terpenting saat mendesain tabel. | `fct_pm25_hourly` = sensor × jam; `fct_pm25_daily` = lokasi × hari |
| **Incremental / windowed** | Jangan download ulang semua sejarah tiap hari. Ambil hanya jendela waktu run itu. | `data_interval_start/end` di DAG |
| **Fail loudly** | Kalau data rusak, pipeline harus gagal dan terlihat, bukan diam-diam salah. | dbt tests; `dbt build` gagal → task gagal |
| **Tahan gangguan sementara** | Internet putus atau server sibuk itu normal. Coba ulang dengan jeda. | Retry di `client.py` + `retries=2` di DAG |
| **Reproducible environment** | "Di laptop saya jalan" tidak cukup. Lingkungan harus bisa dibuat ulang persis. | `Dockerfile`, versi library dipatok (`==`) |
| **Rahasia tidak masuk git** | API key di `.env`, dan `.env` di-ignore. | `.gitignore`, `env_file: .env` |

### 2.4 Pertanyaan yang perlu dijawab untuk setiap pipeline
1. **Apa** yang diambil, dan **seberapa sering**? (PM2.5 Indonesia, harian)
2. **Bagaimana kalau gagal di tengah jalan?** (retry; idempotent sehingga aman diulang)
3. **Bagaimana kalau dijalankan dua kali?** (upsert, tidak duplikat)
4. **Bagaimana saya tahu datanya benar?** (dbt tests)
5. **Siapa yang memakai hasilnya, dan dalam bentuk apa?** (tabel marts, dashboard)

---

## 3. Peta repo dan alur end-to-end

### 3.1 Struktur folder

```
OpenAQProject/
├── Handler/                     # Kode Python Extract + Load
│   ├── client.py                #   Klien API OpenAQ (auth, paginasi, retry)
│   ├── loader.py                #   Menulis ke DuckDB schema raw (upsert)
│   └── main.py                  #   Menjalankan E+L sekali secara manual (tanpa Airflow)
├── openaq_dbt/                  # Proyek dbt (Transform)
│   ├── dbt_project.yml          #   Konfigurasi proyek + variabel ISPU
│   ├── profiles.yml             #   Koneksi dbt ke file DuckDB
│   ├── macros/                  #   generate_schema_name, as_of_utc
│   ├── models/staging/          #   Lapis staging (view)
│   ├── models/marts/            #   Lapis marts (table)
│   └── tests/                   #   Test data custom
├── dags/openaq_dag.py           # Definisi pipeline Airflow (Orkestrasi)
├── dashboard/app.py             # Dashboard Streamlit (Serving)
├── Dockerfile                   # Resep image Airflow + dbt
├── docker-compose.yaml          # Menjalankan Postgres + komponen Airflow
├── .dockerignore                # File yang tidak ikut ke image
├── requirements.txt             # Library untuk venv lokal (dev)
├── requirements-airflow.txt     # Library tambahan untuk image Airflow
├── tests/                       # Unit test Python (pytest) untuk Handler/
├── pyproject.toml               # Konfigurasi pytest
├── .github/workflows/ci.yml     # CI: pytest + dbt build di setiap push/PR
├── data/openaq.duckdb           # Database hasil (tidak di-commit)
└── scripts/                     # Coretan, jalankan dari root: python -m scripts.<nama>
    ├── explore_api.py           # Eksplorasi API
    ├── load_sample_sensor.py    # Tes Handler: load 1 sensor (data Jan–Feb 2016)
    └── inspect_db.py            # Cek isi database
```

### 3.2 Alur data

```
                       ┌─────────────────────┐
                       │   OpenAQ API v3     │
                       └──────────┬──────────┘
                                  │  HTTP GET + X-API-Key
                                  │  (Handler/client.py)
  ┌───────────────────────────────▼──────────────────────────────────┐
  │                    data/openaq.duckdb                            │
  │                                                                  │
  │  RAW (table)          ← Handler/loader.py     [Airflow task 1,2] │
  │    raw.locations, raw.sensors, raw.measurements_hourly           │
  │                │                                                 │
  │                ▼  dbt (view)                  [Airflow task 3]   │
  │  STAGING                                                         │
  │    stg_openaq__locations, stg_openaq__sensors,                   │
  │    stg_openaq__measurements_hourly                               │
  │                │                                                 │
  │                ▼  dbt (table)                                    │
  │  MARTS                                                           │
  │    dim_locations, dim_sensors, fct_pm25_hourly, fct_pm25_daily   │
  └───────────────────────────────┬──────────────────────────────────┘
                                  │ read-only
                                  ▼
                       ┌─────────────────────┐
                       │ dashboard/app.py    │
                       └─────────────────────┘
```

### 3.3 Alur kontrol (siapa menjalankan siapa)

```
docker compose up
   └─ airflow-scheduler (setiap hari)
        └─ DAG openaq_ingest
             ├─ task 1: load_locations_task  → client.locations() → loader.load_locations()
             ├─ task 2: load_hours_task      → client.hours()     → loader.load_hours()
             ├─ task 3: dbt_source_freshness_task → dbt source freshness (data masih baru?)
             └─ task 4: dbt_build_task       → dbt build --vars as_of_utc (run model + test)
```

---

## 4. Tahap 0: Eksplorasi API

**File:** `scripts/explore_api.py`

Sebelum menulis kode yang rapi, kita "colek" API-nya dulu: kirim 1 request, lihat bentuk JSON-nya, dan cek header rate limit.

```python
r = httpx.get(
    "https://api.openaq.org/v3/locations",
    headers=headers,                              # {"X-API-Key": ...}
    params={"limit": 5, "countries_id": 1},       # 1 = Indonesia
    timeout=30,
)
print(r.status_code, r.headers.get("x-ratelimit-remaining"))
```

Dari eksplorasi ini kita belajar bahwa:
- `countries_id=1` adalah Indonesia,
- JSON-nya bertingkat (`coordinates.latitude`, `provider.name`, `sensors[]`),
- ada header `x-ratelimit-remaining` dan `x-ratelimit-reset` yang perlu dihormati.

> **Pola pikir DE:** file di `scripts/` boleh berantakan. Fungsinya untuk memahami sumber data. Hasil pemahamannya kemudian dituangkan ke kode yang rapi (`Handler/`).

---

## 5. Tahap 1: Extract (`Handler/client.py`)

File ini adalah **klien tipis** untuk API: satu tempat yang mengurus autentikasi, paginasi, retry, dan rate limit, sehingga kode lain cukup memanggil `client.locations(...)`.

### 5.1 Konstanta

```python
BASE_URL = "https://api.openaq.org/v3"
ROOT = Path(__file__).resolve().parent.parent
RETRY_STATUS = {429, 500, 502, 503, 504}
```

| Konstanta | Penjelasan |
|---|---|
| `BASE_URL` | Alamat dasar API. Semua path (`/locations`, `/sensors/...`) ditempel di belakangnya. |
| `ROOT` | Folder root proyek (`Handler/..`). Dipakai untuk mencari file `.env`, berapa pun current directory-nya. |
| `RETRY_STATUS` | Kode HTTP yang dianggap **error sementara** sehingga layak dicoba ulang: `429` = terlalu banyak request (rate limit), `5xx` = server bermasalah. Kode lain seperti `401` (key salah) atau `404` tidak di-retry, karena mencoba ulang tidak akan membantu. |

### 5.2 `__init__`: membuat klien

```python
def __init__(self, api_key=None, max_retries=5, timeout=30):
    load_dotenv(ROOT / ".env")
    api_key = api_key or os.getenv("OPENAQ_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAQ_API_KEY tidak ditemukan di environment / .env")
    self.max_retries = max_retries
    self._http = httpx.Client(base_url=BASE_URL, headers={"X-API-Key": api_key}, timeout=timeout)
```

- `load_dotenv(...)` membaca `.env` lalu memasukkannya ke environment variable. Di Docker, variabel ini sudah diisi oleh `env_file: .env`, jadi baris ini hanya berguna saat dijalankan lokal.
- Kalau key tidak ada, program **langsung gagal dengan pesan jelas** (*fail fast*). Ini lebih baik daripada gagal belakangan dengan error 401 yang membingungkan.
- `httpx.Client` dibuat **sekali** dan dipakai ulang. Koneksi TCP-nya ikut dipakai ulang (connection pooling), sehingga lebih cepat daripada membuat koneksi baru di tiap request.

### 5.3 `close`, `__enter__`, `__exit__`: context manager

```python
with OpenAQClient() as client:
    ...
# di sini koneksi HTTP otomatis ditutup, walaupun terjadi error
```

Ketiga method ini membuat klien bisa dipakai dengan `with`, sehingga resource selalu dibersihkan.

### 5.4 `get`: satu request dengan retry

```python
def get(self, path, **params):
    params = {k: v for k, v in params.items() if v is not None}
    for attempt in range(self.max_retries + 1):
        try:
            resp = self._http.get(path, params=params)
        except httpx.TransportError:              # (a) error jaringan
            if attempt == self.max_retries:
                raise
            time.sleep(2**attempt)
            continue

        if resp.status_code in RETRY_STATUS and attempt < self.max_retries:   # (b) 429/5xx
            reset = resp.headers.get("x-ratelimit-reset")
            wait = float(reset) + 1 if reset and resp.status_code == 429 else 2**attempt
            time.sleep(wait)
            continue

        resp.raise_for_status()                    # (c) error lain → lempar exception
        if resp.headers.get("x-ratelimit-remaining") == "0":                  # (d) rem kuota
            time.sleep(float(resp.headers.get("x-ratelimit-reset", 1)) + 1)
        return resp.json()
```

Penjelasan per baris:

- **Filter `None`**: parameter opsional yang tidak diisi tidak ikut dikirim ke API.
- **Loop `max_retries + 1`**: total 1 percobaan awal ditambah 5 retry.
- **(a) `TransportError`** (koneksi putus, timeout): tunggu `2**attempt` detik (1, 2, 4, 8, 16). Ini disebut **exponential backoff**, yaitu jeda yang makin lama supaya tidak membanjiri server. Pada percobaan terakhir, error dilempar.
- **(b) Status 429/5xx**: kalau 429, tunggu sesuai header `x-ratelimit-reset` (detik sampai kuota reset) ditambah 1 detik. Kalau 5xx, pakai exponential backoff.
- **(c) `raise_for_status()`**: status error yang tidak bisa di-retry (atau retry sudah habis) dijadikan exception.
- **(d) Proaktif**: kalau sisa kuota sudah `0`, tunggu dulu sebelum request berikutnya, sehingga kita tidak sampai kena 429.

### 5.5 `paginate`: mengambil semua halaman

```python
def paginate(self, path, limit=1000, **params):
    page = 1
    while True:
        body = self.get(path, limit=limit, page=page, **params)
        results = body.get("results", [])
        yield from results
        if len(results) < limit:
            return
        page += 1
```

- API hanya mengembalikan maksimal `limit` baris per halaman.
- Fungsi ini adalah **generator** (`yield`): baris dikirim satu per satu ke pemanggil, dan halaman berikutnya baru diambil saat dibutuhkan (*lazy*). Memori tetap hemat walaupun datanya besar.
- **Kondisi berhenti**: kalau satu halaman berisi kurang dari `limit`, berarti itu halaman terakhir.

### 5.6 Method endpoint

```python
def countries(self):                   return self.paginate("/countries")
def locations(self, countries_id):     return self.paginate("/locations", countries_id=countries_id)
def hours(self, sensor_id, datetime_from=None, datetime_to=None):
    return self.paginate(f"/sensors/{sensor_id}/hours", datetime_from=..., datetime_to=...)
```

| Method | Endpoint | Isi |
|---|---|---|
| `countries()` | `/countries` | Daftar negara (untuk mencari ID Indonesia) |
| `locations(countries_id)` | `/locations` | Stasiun di suatu negara, **termasuk daftar sensornya** |
| `hours(sensor_id, from, to)` | `/sensors/{id}/hours` | Agregat **per jam** satu sensor dalam rentang waktu |

> **Pola pikir DE:** pisahkan "cara bicara dengan sumber" (client) dari "cara menyimpan" (loader). Kalau API berganti versi, cukup `client.py` yang diubah.

---

## 6. Tahap 2: Load ke RAW (`Handler/loader.py`)

File ini menerima data dari client lalu menulisnya ke DuckDB schema `raw` secara **idempotent**.

### 6.1 `DB_PATH`

```python
DB_PATH = Path(os.getenv("OPENAQ_DB_PATH", Path(__file__).resolve().parent.parent / "data" / "openaq.duckdb"))
```

- Kalau env `OPENAQ_DB_PATH` ada (di Docker: `/opt/airflow/data/openaq.duckdb`), path itu yang dipakai.
- Kalau tidak ada (lokal), dipakai `<root>/data/openaq.duckdb`.
- Kode yang sama bisa jalan di laptop maupun di container hanya dengan mengganti environment variable. Pola ini disebut **konfigurasi lewat environment**.

### 6.2 `DDL`: struktur tabel raw

`DDL` (Data Definition Language) adalah SQL yang membuat schema dan tabel. Pemakaian `IF NOT EXISTS` membuatnya aman dijalankan berkali-kali.

**`raw.locations`**: satu baris per stasiun.

| Kolom | Asal JSON | Keterangan |
|---|---|---|
| `location_id` **PK** | `id` | Kunci unik |
| `name` | `name` | Nama stasiun |
| `country_code` | `country.code` | mis. `ID` |
| `timezone` | `timezone` | mis. `Asia/Jakarta`, dipakai untuk konversi waktu lokal |
| `latitude`, `longitude` | `coordinates.*` | Untuk peta dan filter bounding box |
| `is_mobile`, `is_monitor` | `isMobile`, `isMonitor` | Sensor bergerak? Monitor referensi resmi? |
| `provider_name` | `provider.name` | Penyedia data |
| `datetime_first_utc`, `datetime_last_utc` | `datetimeFirst.utc`, `datetimeLast.utc` | Kapan pertama dan terakhir kirim data (dipakai untuk "aktif") |
| `payload` | seluruh objek | **JSON asli**, sebagai cadangan kalau nanti butuh field lain |
| `_loaded_at` | — | Kapan baris ini terakhir dimuat (metadata pipeline) |

**`raw.sensors`**: satu baris per sensor. Data ini diambil dari `sensors[]` **di dalam** respons `/locations`, jadi tidak perlu request terpisah.

| Kolom | Keterangan |
|---|---|
| `sensor_id` **PK** | Kunci unik |
| `location_id` | Sensor ini milik stasiun mana (foreign key logis) |
| `parameter_id`, `parameter_name`, `units` | mis. `2`, `pm25`, `µg/m³` |

**`raw.measurements_hourly`**: satu baris per sensor per jam.

| Kolom | Keterangan |
|---|---|
| `sensor_id`, `datetime_from_utc` **PK gabungan** | Grain: sensor × jam |
| `datetime_to_utc` | Akhir periode jam |
| `value` | Nilai utama (rata-rata jam itu) |
| `min`, `max`, `avg`, `sd` | Statistik ringkas dalam jam itu |
| `percent_coverage` | Persentase data mentah yang tersedia dalam jam itu |
| `has_flags` | Apakah OpenAQ menandai data ini bermasalah |
| `payload`, `_loaded_at` | JSON asli + metadata |

> **Kenapa PRIMARY KEY penting?** Tanpa PK, `INSERT OR REPLACE` tidak tahu baris mana yang "sama", sehingga data bisa dobel tiap kali pipeline jalan ulang.

### 6.3 `_g`: ambil nilai nested dengan aman

```python
def _g(d, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d
```

`_g(loc, "coordinates", "latitude")` sama dengan `loc["coordinates"]["latitude"]`, tapi **tidak error** kalau `coordinates` tidak ada atau bernilai `null`. Hasilnya cukup `None`. Data API di dunia nyata sering tidak lengkap, jadi helper seperti ini penting.

### 6.4 `connect`

```python
def connect(path=DB_PATH):
    Path(path).parent.mkdir(parents=True, exist_ok=True)   # buat folder data/ bila belum ada
    con = duckdb.connect(str(path))                        # buka (atau buat) file .duckdb
    con.execute(DDL)                                       # pastikan schema & tabel ada
    return con
```

Siapa pun yang memanggil `connect()` dijamin mendapat database yang tabelnya sudah siap. Pemanggil tidak perlu memikirkan setup.

### 6.5 `load_locations`

```python
def load_locations(con, locations) -> tuple[int, int]:
    loc_rows, sensor_rows = [], []
    for loc in locations:                       # iterasi generator dari client.paginate
        loc_rows.append((loc["id"], loc.get("name"), _g(loc, "country", "code"), ...,
                         json.dumps(loc)))      # payload = JSON utuh
        for s in loc.get("sensors") or []:      # sensor ada di dalam location
            sensor_rows.append((s["id"], loc["id"], s.get("name"), _g(s, "parameter", "id"), ...))

    if not loc_rows:                            # DuckDB executemany menolak list kosong
        return 0, 0
    con.executemany("INSERT OR REPLACE INTO raw.locations (...) VALUES (?,?,...,?, now())", loc_rows)
    if sensor_rows:
        con.executemany("INSERT OR REPLACE INTO raw.sensors (...) VALUES (?,...,?, now())", sensor_rows)
    return len(loc_rows), len(sensor_rows)
```

Hal penting di fungsi ini:
- **Flattening**: JSON bertingkat diubah menjadi baris-baris tabel (tuple).
- **Satu respons, dua tabel**: lokasi masuk ke `raw.locations`, sensor di dalamnya masuk ke `raw.sensors`.
- **`?` (parameter placeholder)**: nilai dikirim terpisah dari SQL, sehingga aman dari SQL injection dan tidak perlu memikirkan escape tanda kutip.
- **`executemany`**: insert banyak baris sekaligus, jauh lebih cepat daripada satu per satu.
- **`INSERT OR REPLACE` = upsert**: kalau PK sudah ada, baris diganti dengan versi terbaru. Kalau belum ada, baris ditambahkan.
- **`_loaded_at` diisi `now()` secara eksplisit**: pada `INSERT OR REPLACE`, nilai `DEFAULT now()` tidak diterapkan ulang saat baris sudah ada. Karena itu nilainya ditulis langsung di `VALUES`. Kolom ini dipakai dbt untuk mengecek **freshness** (apakah data masih segar).

### 6.6 `load_hours`

```python
def load_hours(con, sensor_id, rows) -> int:
    data = [(sensor_id, _g(r, "period", "datetimeFrom", "utc"), ..., json.dumps(r)) for r in rows]
    if not data:
        return 0
    con.executemany("INSERT OR REPLACE INTO raw.measurements_hourly (...) VALUES (...)", data)
    return len(data)
```

Polanya sama dengan `load_locations`, untuk data per jam **satu sensor**. `sensor_id` diambil dari argumen karena respons `/sensors/{id}/hours` tidak selalu menyertakan ID sensor di tiap baris.

> **Pola pikir DE:** fungsi load **menerima** data (iterable) dan **mengembalikan** jumlah baris. Ia tidak tahu dari mana datanya berasal. Karena itu fungsi ini mudah dites dengan data palsu, dan angka yang dikembalikan berguna untuk log dan monitoring.

---

## 7. Menjalankan Extract dan Load secara manual (`Handler/main.py`)

Script CLI untuk menjalankan E+L **sekali**, tanpa Airflow. Berguna untuk belajar dan debugging.

```bash
python -m Handler.main
python -m Handler.main --country-id 1 --sensor-id 5047 --from 2016-01-30 --to 2016-02-06
```

| Argumen | Default | Arti |
|---|---|---|
| `--country-id` | `1` | Indonesia |
| `--sensor-id` | `5047` | Sensor PM2.5 US Diplomatic Post Jakarta South |
| `--from`, `--to` | `2016-01-30` s/d `2016-02-05` | Rentang data per jam |

Alur `main()`: `connect()` → `load_locations(...)` → `load_hours(...)` untuk satu sensor → cetak jumlah baris → `close()`.

Perhatikan bahwa DAG Airflow memanggil **fungsi yang sama persis** (`load_locations`, `load_hours`). Airflow hanya mengganti *siapa yang memanggil dan kapan*, bukan *logikanya*.

---

## 8. Tahap 3: Transform dengan dbt (`openaq_dbt/`)

### 8.1 Apa itu dbt?
dbt (*data build tool*) memungkinkan kita menulis transformasi sebagai **file `SELECT` SQL**. dbt yang mengurus:
- membuat `VIEW` / `TABLE` dari tiap file (disebut **model**),
- menentukan **urutan** pembuatan berdasarkan `ref()` (dbt membangun DAG-nya sendiri),
- menjalankan **test** kualitas data.

Perintah utama: `dbt build` = jalankan semua model + semua test sesuai urutan dependensi.

### 8.2 `dbt_project.yml`

```yaml
models:
  openaq_dbt:
    staging:
      +schema: staging          # model di folder staging/ → schema "staging"
      +materialized: view       # dibuat sebagai VIEW
    marts:
      +schema: marts
      +materialized: table      # dibuat sebagai TABLE

vars:
  ispu_pm25_breakpoints: [15.5, 55.4, 150.4, 250.4]
```

- **Kenapa staging = view?** Staging hanya rename dan casting, jadi ringan. View tidak memakan ruang dan selalu up-to-date terhadap raw.
- **Kenapa marts = table?** Marts dipakai berulang kali oleh dashboard atau analis, dan isinya ada join + agregasi. Menyimpannya sebagai table membuat query lebih cepat.
- **`vars.ispu_pm25_breakpoints`**: batas kategori ISPU untuk PM2.5 rata-rata 24 jam (µg/m³), mengacu ke Permen LHK No. 14/2020. Disimpan sebagai variabel supaya **tidak di-hardcode** di SQL, dan cukup diubah di satu tempat kalau regulasinya berubah.

### 8.3 `profiles.yml`: koneksi

```yaml
openaq_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('OPENAQ_DB_PATH', '../data/openaq.duckdb') }}"
      threads: 1
```

- `path` membaca env `OPENAQ_DB_PATH` (Docker). Kalau tidak ada, dipakai `../data/openaq.duckdb` (lokal, dijalankan dari folder `openaq_dbt/`).
- `threads: 1`: DuckDB hanya mengizinkan **satu penulis** per file, jadi model dibangun satu per satu.

### 8.4 `macros/generate_schema_name.sql`

Secara default, dbt menamai schema custom sebagai `<schema_target>_<custom>`, misalnya `main_staging`. Macro ini menimpa perilaku tersebut supaya nama schema-nya persis `staging` dan `marts`.

### 8.5 Sources: `models/staging/_sources.yml`

```yaml
sources:
  - name: openaq
    schema: raw
    config:
      loaded_at_field: _loaded_at
      freshness:
        warn_after:  {count: 2, period: day}
        error_after: {count: 4, period: day}
    tables: [locations, sensors, measurements_hourly]   # + test unique/not_null
```

- **Source** mendaftarkan tabel yang **bukan dibuat dbt** (di sini: tabel raw dari Python) supaya bisa dipanggil dengan `{{ source('openaq', 'locations') }}`.
- **Freshness**: `dbt source freshness` akan *warn* kalau `_loaded_at` terbaru lebih tua dari 2 hari, dan *error* kalau lebih dari 4 hari. Ini tanda bahwa ingest berhenti. (Catatan: perintah ini belum dijalankan di DAG. Lihat Bagian 13.)
- Test `unique` + `not_null` pada PK raw memastikan loader bekerja benar.

### 8.6 Lapis STAGING: rapikan saja

Aturan staging: **satu model per tabel source**, hanya rename, casting, dan pembersihan ringan. **Tidak ada join dan tidak ada logika bisnis.**

**`stg_openaq__locations.sql`**
```sql
trim(name)                             as location_name,     -- buang spasi di awal/akhir
upper(country_code)                    as country_code,      -- seragamkan huruf
coalesce(is_mobile, false)             as is_mobile,         -- NULL → false
datetime_first_utc at time zone 'UTC'  as first_seen_at_utc, -- TIMESTAMPTZ → TIMESTAMP UTC
datetime_last_utc  at time zone 'UTC'  as last_seen_at_utc,
```

**`stg_openaq__sensors.sql`**
```sql
name                   as sensor_name,
lower(parameter_name)  as parameter_name,   -- 'PM25' / 'pm25' → 'pm25'
```

**`stg_openaq__measurements_hourly.sql`**
```sql
datetime_from_utc at time zone 'UTC' as measured_at_utc,   -- nama kolom lebih jelas
min as value_min, max as value_max, avg as value_avg, sd as value_sd,  -- hindari nama yang bentrok dengan fungsi SQL
coalesce(has_flags, false)           as has_flags,
```

> **Kenapa waktu dijadikan `TIMESTAMP` UTC tanpa zona?** `TIMESTAMPTZ` ditampilkan mengikuti zona waktu sesi/komputer, sehingga hasilnya bisa beda di laptop dan di container. Dengan menyimpannya sebagai UTC "polos" dan konsisten, konversi ke waktu lokal dilakukan secara eksplisit di marts.

**Test di `_staging.yml`:**
- `unique` / `not_null` pada ID,
- `relationships`: setiap `sensor.location_id` harus ada di `stg_openaq__locations`, dan setiap `measurement.sensor_id` harus ada di `stg_openaq__sensors` (integritas referensial),
- `non_negative` pada `value` dengan `severity: warn`: nilai negatif hanya memunculkan peringatan, tidak menggagalkan pipeline, karena data sensor mentah kadang memang aneh.

### 8.7 Lapis MARTS: logika bisnis

Konvensi nama:
- **`dim_*` (dimension)**: tabel "kata benda", yaitu deskripsi entitas (lokasi, sensor). Perubahannya lambat.
- **`fct_*` (fact)**: tabel "kejadian" atau pengukuran yang bertambah terus (nilai PM2.5 per jam/hari).

#### `dim_locations.sql`
```sql
select ...,
    {{ as_of_utc() }}                                      as as_of_utc,
    last_seen_at_utc >= {{ as_of_utc() }} - interval 7 day as is_active,
    coalesce(
        latitude between -11.5 and 6.5 and longitude between 94.5 and 141.5,
        false
    )                                                                 as is_within_indonesia
from {{ ref('stg_openaq__locations') }}
```
- `ref('...')` memberi tahu dbt bahwa model ini bergantung pada `stg_openaq__locations`, sehingga model itu dibangun duluan.
- `is_active`: lokasi yang mengirim data dalam 7 hari sebelum **waktu acuan** `as_of_utc`. Macro `macros/as_of_utc.sql` membaca var dbt `as_of_utc`, yang dikirim Airflow berisi `data_interval_end`. Kalau var tidak ada (dbt dijalankan manual di laptop), fallback ke `now()`.
  > **Pola pikir DE:** model sebaiknya **deterministik**: input sama + run sama → output sama. Dengan `now()`, menjalankan ulang run tanggal 1 pada tanggal 10 memberi `is_active` yang berbeda. Dengan waktu acuan dari run, rerun dan backfill konsisten. Kolom `as_of_utc` ikut disimpan supaya pemakai tabel tahu acuannya.
- `is_within_indonesia`: **aturan kualitas data**. OpenAQ kadang salah tag negara (mis. lokasi 6315887 di Bulgaria bertag `ID`). Bounding box kasar Indonesia (lintang −11.5 s/d 6.5, bujur 94.5 s/d 141.5) dipakai untuk menyaringnya. `coalesce(..., false)` membuat koordinat kosong dianggap di luar.

#### `dim_sensors.sql`
Sensor di-`left join` dengan lokasi untuk menambahkan `location_name`. Dipakai `left join` supaya sensor tetap muncul walaupun lokasinya tidak ditemukan.

#### `fct_pm25_hourly.sql`: grain = sensor × jam
```sql
select
    m.sensor_id, s.location_id, m.measured_at_utc,
    timezone(l.timezone, timezone('UTC', m.measured_at_utc))  as measured_at_local,
    m.value as pm25_ugm3, m.percent_coverage, m.has_flags
from stg_openaq__measurements_hourly m
join stg_openaq__sensors s using (sensor_id)        -- sensor → location_id
join dim_locations       l using (location_id)      -- location → timezone, filter negara
where m.parameter_name = 'pm25'
  and m.value is not null
  and l.is_within_indonesia
```
- **Konversi waktu lokal** (dibaca dari dalam ke luar): `timezone('UTC', ts)` menyatakan "timestamp polos ini adalah UTC", lalu `timezone(l.timezone, ...)` mengubahnya ke jam lokal stasiun (WIB/WITA/WIT). Ini penting karena "hari" menurut orang Jakarta berbeda dengan hari UTC.
- **Filter**: hanya PM2.5, nilai tidak kosong, dan lokasi di dalam Indonesia.
- `join` biasa (inner) membuang pengukuran yang sensor atau lokasinya tidak dikenal.

#### `fct_pm25_daily.sql`: grain = lokasi × tanggal lokal
```sql
{% set bp = var('ispu_pm25_breakpoints') %}          -- Jinja: ambil variabel dari dbt_project.yml

with daily as (
    select location_id,
           cast(measured_at_local as date) as date_local,
           avg(pm25_ugm3) as pm25_avg, max(...) as pm25_max, min(...) as pm25_min,
           count(distinct date_trunc('hour', measured_at_local)) as n_hours,  -- jam unik
           count(*) as n_sensor_hours, count(distinct sensor_id) as n_sensors
    from {{ ref('fct_pm25_hourly') }}
    group by all                                      -- DuckDB: group by semua kolom non-agregat
)
select *,
    n_hours >= 18 as is_complete_day,                 -- ~75% jam terisi
    case
        when pm25_avg <= {{ bp[0] }} then 'Baik'               -- ≤ 15.5
        when pm25_avg <= {{ bp[1] }} then 'Sedang'             -- ≤ 55.4
        when pm25_avg <= {{ bp[2] }} then 'Tidak Sehat'        -- ≤ 150.4
        when pm25_avg <= {{ bp[3] }} then 'Sangat Tidak Sehat' -- ≤ 250.4
        else 'Berbahaya'
    end as ispu_category
from daily
```
- **Jinja** (`{% %}`, `{{ }}`) adalah templating dbt. Sebelum SQL dijalankan, `{{ bp[0] }}` diganti menjadi `15.5`, dan seterusnya.
- **CTE `with daily as (...)`**: memecah query menjadi langkah yang mudah dibaca (agregasi dulu, baru kategorisasi).
- `n_hours` vs `n_sensor_hours`: kalau satu lokasi punya 2 sensor, satu jam bisa punya 2 baris. `count(*)` akan menghitung 48 untuk sehari penuh, sehingga `n_hours` dihitung dari jam unik. Test `tests/assert_daily_hours_within_day.sql` menjaga agar nilainya tetap 1–24.
- `is_complete_day`: hari dengan data kurang dari 18 jam rata-ratanya kurang representatif, sehingga ditandai, bukan dibuang. Pemakai data yang memutuskan.

**Test di `_marts.yml` dan `tests/`:**

| Test | Memastikan |
|---|---|
| `unique`/`not_null` pada `dim_*` | Satu baris per lokasi atau sensor |
| `relationships` fct → dim | Tidak ada fakta "yatim" (tanpa dimensi) |
| `non_negative` pada `pm25_ugm3` | PM2.5 tidak negatif (di sini **error**, bukan warn) |
| `accepted_values` pada `ispu_category` | Hanya 5 kategori yang sah |
| `tests/assert_hourly_grain_unique.sql` | **Grain terjaga**: tidak ada sensor + jam yang muncul dua kali. Test custom (*singular test*) lulus kalau query-nya mengembalikan 0 baris. |
| `tests/generic/non_negative.sql` | Definisi test generik buatan sendiri, bisa dipakai di kolom mana pun |

> **Pola pikir DE:** test adalah **kontrak**. Kalau suatu saat API mengirim data aneh, pipeline gagal dan kita tahu, daripada dashboard diam-diam menampilkan angka salah.

### 8.8 Urutan yang dibangun dbt (DAG internal dbt)

```
source raw.* ──► stg_locations ──► dim_locations ──┐
              ├► stg_sensors ─────► dim_sensors     ├──► fct_pm25_hourly ──► fct_pm25_daily
              └► stg_measurements_hourly ──────────┘
```
Kita tidak menulis urutan ini secara manual. dbt menyimpulkannya dari `ref()` dan `source()`.

---

## 9. Tahap 4: Orkestrasi dengan Airflow (`dags/openaq_dag.py`)

### 9.1 Konsep
- **DAG (Directed Acyclic Graph)**: kumpulan task beserta urutannya, tanpa siklus.
- **Task**: satu unit kerja (satu fungsi Python atau satu perintah bash).
- **Run (DAG run)**: satu kali eksekusi DAG, misalnya "run untuk tanggal 25 November".
- **Data interval**: rentang waktu data yang menjadi tanggung jawab satu run. Untuk `@daily`, run "tanggal 25" mencakup `25 Nov 00:00 → 26 Nov 00:00` (UTC) dan **baru dijalankan setelah interval itu selesai**, karena datanya baru lengkap saat itu.

### 9.2 Konstanta

```python
COUNTRY_ID = 1          # Indonesia
PARAMETER = "pm25"
ACTIVE_WITHIN_DAYS = 7  # hanya sensor yang lokasinya masih kirim data belakangan ini
```
Ketiganya ditaruh di atas sebagai konstanta supaya mudah diubah tanpa mencari di dalam logika.

### 9.3 Decorator `@dag`

```python
@dag(
    dag_id="openaq_ingest",
    schedule="@daily",                        # jalan sekali sehari
    start_date=datetime(2026, 11, 1),         # tanggal mulai berlaku
    catchup=False,                            # jangan kejar run yang terlewat sejak start_date
    max_active_runs=1,                        # hanya 1 run bersamaan (DuckDB 1 penulis)
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["openaq", "duckdb"],
)
```

| Parameter | Kenapa begitu |
|---|---|
| `catchup=False` | Kalau `True`, Airflow akan membuat run untuk setiap hari sejak 1 Nov yang belum jalan. Untuk belajar, ini dimatikan. |
| `max_active_runs=1` | Dua run bersamaan akan berebut kunci file DuckDB dan salah satunya gagal. |
| `retries=2` | Error sementara (internet, API down) dicoba ulang 2x dengan jeda 5 menit. |

### 9.4 Task 1: `load_locations_task`

```python
@task
def load_locations_task() -> dict:
    from Handler.client import OpenAQClient        # import DI DALAM fungsi
    from Handler.loader import connect, load_locations
    con = connect()
    try:
        with OpenAQClient() as client:
            n_loc, n_sensor = load_locations(con, client.locations(countries_id=COUNTRY_ID))
    finally:
        con.close()                                 # lepas kunci DuckDB walau gagal
    return {"locations": n_loc, "sensors": n_sensor}
```
- **Import di dalam fungsi**: file DAG di-parse ulang secara berkala oleh dag-processor. Import yang berat di level atas akan memperlambat parsing. Dengan cara ini, import hanya terjadi saat task benar-benar jalan.
- **`try/finally` + `con.close()`**: kalau koneksi tidak ditutup saat error, file DuckDB tetap terkunci dan task berikutnya ikut gagal.
- **`return dict`**: nilai yang dikembalikan disimpan Airflow sebagai **XCom** dan bisa dilihat di UI. Berguna untuk monitoring jumlah baris.
- Task ini selalu mengambil **semua** lokasi (bukan incremental), karena jumlahnya kecil (~140) dan metadata seperti `datetime_last_utc` perlu diperbarui.

### 9.5 Task 2: `load_hours_task`

```python
@task
def load_hours_task(**context) -> int:
    end = context.get("data_interval_end") or pendulum.now("UTC")
    start = context.get("data_interval_start")
    if start is None or start >= end:
        start = end.subtract(days=1)
```
- **`**context`**: Airflow menyuntikkan informasi run, termasuk data interval.
- **Run terjadwal**: memakai interval resmi (kemarin 00:00 → hari ini 00:00).
- **Run manual** (tombol Trigger): interval bisa kosong atau `start == end`, sehingga dipakai fallback 24 jam terakhir.

```python
    sensor_ids = [r[0] for r in con.execute("""
        SELECT s.sensor_id
        FROM raw.sensors s JOIN raw.locations l USING (location_id)
        WHERE s.parameter_name = ?
          AND l.datetime_last_utc >= CAST(? AS TIMESTAMPTZ) - to_days(CAST(? AS INTEGER))
        ORDER BY s.sensor_id
    """, [PARAMETER, date_to, ACTIVE_WITHIN_DAYS]).fetchall()]
```
- Daftar sensor diambil dari **hasil task 1** di `raw`. Inilah alasan task 2 harus jalan setelah task 1.
- Hanya sensor **PM2.5** di lokasi yang **aktif dalam 7 hari sebelum akhir window** (`date_to`, bukan `now()`). Sensor mati tidak perlu ditanya, sehingga request ke API dan kuota rate limit lebih hemat. Karena acuannya akhir window, backfill tanggal lama tetap memilih sensor yang aktif pada periode itu.

```python
    with OpenAQClient() as client:
        for sid in sensor_ids:
            n = load_hours(con, sid, client.hours(sid, date_from, date_to))
            total += n
```
- Satu request (atau lebih, kalau ada paginasi) per sensor untuk jendela waktu run itu.
- Karena upsert, kalau task ini di-retry atau run diulang, tidak ada data yang dobel.

### 9.6 Task 3: `dbt_source_freshness_task`

```python
@task.bash(retries=0)
def dbt_source_freshness_task() -> str:
    return f'cd "$DBT_PROJECT_DIR" && {DBT} source freshness {DBT_PATHS}'
```
- Memeriksa **seberapa baru** data di `raw` sebelum diolah. Aturannya ada di `_sources.yml`:
  - `locations` dan `sensors`: diukur dari `_loaded_at` (kapan loader terakhir menulis). Warn > 2 hari, error > 4 hari.
  - `measurements_hourly`: diukur dari **`datetime_from_utc`**, yaitu jam pengukuran terbaru. Warn > 1 hari, error > 3 hari. Normalnya data telat sekitar 2 jam.
- Kenapa `measurements_hourly` tidak memakai `_loaded_at`? Karena task freshness jalan **tepat setelah** loader, sehingga `_loaded_at` selalu baru dan cek-nya tidak berarti. Yang ingin kita tangkap adalah kasus "loader sukses, tapi API tidak mengirim jam baru" (sensor mati massal, API bermasalah).
- `warn` hanya tercatat di log; `error` menggagalkan task sehingga `dbt_build_task` tidak jalan.

### 9.7 Task 4: `dbt_build_task`

```python
@task.bash(retries=0)
def dbt_build_task(**context) -> str:
    as_of = (context.get("data_interval_end") or pendulum.now("UTC")).isoformat()
    vars_json = json.dumps({"as_of_utc": as_of})
    return f"cd \"$DBT_PROJECT_DIR\" && {DBT} build {DBT_PATHS} --vars '{vars_json}'"
```
- **`@task.bash`**: fungsi mengembalikan **string perintah shell**, lalu Airflow yang menjalankannya.
- **`--vars`**: mengirim `as_of_utc` = akhir data interval ke dbt, sehingga `is_active` di `dim_locations` deterministik per run (lihat 8.7).
- **`/opt/dbt_venv/bin/dbt`**: dbt dari virtualenv terpisah (lihat Dockerfile).
- **`--target-path /tmp/...`**: file hasil kompilasi dbt ditaruh di `/tmp` container, supaya folder proyek yang di-mount dari laptop tetap bersih.
- **`retries=0`**: kalau dbt gagal, penyebabnya biasanya **test data gagal**, yaitu masalah data, bukan gangguan sementara. Retry tidak akan membantu, jadi lebih baik langsung gagal supaya diperiksa.

### 9.8 Dependensi

```python
load_locations_task() >> load_hours_task() >> dbt_source_freshness_task() >> dbt_build_task()
openaq_ingest()   # panggil fungsi DAG supaya terdaftar di Airflow
```
`>>` artinya "kanan jalan setelah kiri sukses".

---

## 10. Tahap 5: Environment dengan Docker

### 10.1 Kenapa Docker?
Airflow butuh Linux, database metadata (Postgres), dan beberapa proses yang berjalan bersamaan, sementara versi library-nya sangat sensitif. Docker membungkus semuanya sehingga cukup **satu perintah** untuk menyalakan semuanya, dan hasilnya sama di komputer mana pun.

- **Image**: cetakan/resep (statis).
- **Container**: image yang sedang berjalan.
- **Volume / bind mount**: folder yang dibagi antara laptop dan container.

### 10.2 `Dockerfile`

```dockerfile
FROM apache/airflow:3.1.0                                  # (1) basis: image resmi Airflow
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt # (2) duckdb, httpx, python-dotenv

USER root
RUN python -m venv /opt/dbt_venv && chown -R airflow:0 /opt/dbt_venv   # (3) venv khusus dbt
USER airflow
RUN /opt/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.7 dbt-duckdb==1.11.1 duckdb==2.0.0
```
1. Mulai dari image resmi yang sudah berisi Airflow 3.1.0.
2. Tambahkan library yang dibutuhkan kode `Handler/`.
3. **dbt di venv terpisah**: dependensi dbt sering bentrok dengan versi library yang dipatok Airflow. Dengan venv sendiri, keduanya tidak saling mengganggu. Pembuatan folder di `/opt` butuh `root`, lalu kepemilikannya diserahkan ke user `airflow`.
4. **Versi `duckdb` harus sama** di loader (2.0.0) dan dbt (2.0.0), karena format file `.duckdb` bisa berbeda antar versi.

`--no-cache-dir` membuat ukuran image lebih kecil (cache pip tidak disimpan).

### 10.3 `.dockerignore`
Daftar file yang **tidak** dikirim ke Docker saat build: `.venv`, `.env` (rahasia!), `data/`, `logs/`, file scratch, dan lainnya. Build jadi lebih cepat dan rahasia tidak masuk ke image. Kode, dbt, dan data tidak perlu masuk image karena di-mount saat runtime.

### 10.4 `docker-compose.yaml`

#### Blok bersama `x-airflow-common`
```yaml
x-airflow-common: &airflow-common     # "&" = anchor YAML, dipakai ulang dengan "<<: *airflow-common"
  build: .                            # build dari Dockerfile di folder ini
  image: openaq-airflow:latest
  env_file: .env                      # memasukkan OPENAQ_API_KEY ke container
```

**Environment variable penting:**

| Variabel | Arti |
|---|---|
| `AIRFLOW__CORE__EXECUTOR: LocalExecutor` | Task dijalankan sebagai proses di mesin yang sama (cukup untuk lokal) |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Alamat Postgres untuk **metadata Airflow** (status run, riwayat). *Bukan* data polusi. |
| `AIRFLOW__CORE__LOAD_EXAMPLES: "false"` | Jangan tampilkan puluhan DAG contoh |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"` | DAG baru dalam keadaan *pause*, jadi harus di-unpause manual di UI (aman) |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | Di Airflow 3, task berkomunikasi dengan API server lewat alamat ini |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Kunci untuk token antar komponen. **Hanya untuk dev lokal.** |
| `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` | Tanpa login, semua dianggap admin (dev lokal) |
| `PYTHONPATH: /opt/airflow` | Supaya `from Handler.client import ...` bisa ditemukan |
| `OPENAQ_DB_PATH` | Lokasi file DuckDB di dalam container (dibaca `loader.py` dan `profiles.yml`) |
| `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR` | Lokasi proyek dbt dan `profiles.yml` |
| `DBT_SEND_ANONYMOUS_USAGE_STATS: "false"` | Matikan telemetri dbt |

**Volumes (bind mount):**
```yaml
- ./dags:/opt/airflow/dags                 # edit DAG di laptop → langsung terbaca Airflow
- ./Handler:/opt/airflow/Handler
- ./openaq_dbt:/opt/airflow/openaq_dbt
- ./data:/opt/airflow/data                 # file openaq.duckdb tersimpan di LAPTOP
- ./logs:/opt/airflow/logs                 # log task bisa dibuka dari laptop
```
Karena di-mount, mengubah kode **tidak perlu rebuild image**. Rebuild hanya perlu kalau `Dockerfile` atau `requirements-airflow.txt` berubah.

`user: "50000:0"`: UID default user `airflow` di image resmi, supaya file yang ditulis container punya izin yang konsisten.

`depends_on ... condition: service_healthy`: komponen Airflow baru start setelah Postgres benar-benar siap.

#### Services

| Service | Fungsi |
|---|---|
| `postgres` | Database metadata Airflow. Datanya di volume `postgres-db` sehingga tidak hilang saat container dimatikan. Punya `healthcheck` `pg_isready`. |
| `airflow-init` | Dijalankan **sekali**: `airflow db migrate` membuat atau memperbarui tabel metadata di Postgres. `restart: "no"`. |
| `airflow-apiserver` | Web UI + REST API. Port `8089` (laptop) → `8080` (container). Buka http://localhost:8089 |
| `airflow-scheduler` | Memutuskan kapan sebuah run dibuat dan menjalankan task-nya (LocalExecutor) |
| `airflow-dag-processor` | Membaca dan mem-parse file di `dags/`, lalu menyimpan strukturnya ke metadata DB |
| `airflow-triggerer` | Untuk *deferrable task* (task yang menunggu tanpa memakan worker). Belum dipakai, tapi bagian standar Airflow 3. |

### 10.5 Dua database, dua fungsi berbeda (sering membingungkan)

```
Postgres   = buku catatan Airflow (run mana sukses/gagal, kapan, log metadata)
DuckDB     = data polusi kita (raw, staging, marts)
```

---

## 11. Tahap 6: Serving / Dashboard (`dashboard/app.py`)

Dashboard Streamlit yang **hanya membaca** DuckDB.

### 11.1 Bagian penting

```python
DB_PATH = Path(os.getenv("OPENAQ_DB_PATH", ROOT / "data" / "openaq.duckdb"))

@st.cache_data(ttl=60)
def query(sql, params=()):
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql, list(params)).df()
    finally:
        con.close()
```
- **`read_only=True` + langsung `close()`**: DuckDB hanya mengizinkan satu penulis. Kalau dashboard membiarkan koneksi terbuka, task Airflow yang mau menulis akan gagal. Karena itu koneksi dibuka dan ditutup di tiap query.
- **`@st.cache_data(ttl=60)`**: hasil query disimpan 60 detik, sehingga interaksi UI tidak membuka file berulang-ulang.
- Kalau file sedang dikunci, `duckdb.IOException` ditangkap dan ditampilkan sebagai pesan ramah.

### 11.2 Isi tab

| Tab | Isi | Tujuan belajar |
|---|---|---|
| 1. Alur pipeline | Diagram raw → staging → marts dengan jumlah baris asli, plus rincian kenapa baris berkurang | Memahami layering dan filter |
| 2. Lacak satu data | Satu pengukuran ditampilkan di raw (+ JSON asli) → staging → fct_hourly → fct_daily, beserta SQL dbt-nya | Melihat transformasi baris demi baris |
| 3. Dashboard PM2.5 | KPI, grafik harian per lokasi, kategori ISPU, peta stasiun, tabel hari terakhir | Seperti apa produk akhirnya |
| 4. Jelajah tabel | Intip tabel mana pun + kotak SQL bebas | Latihan query |

---

## 12. Cara menjalankan

### 12.1 Persiapan (sekali)
1. Daftar API key di OpenAQ, lalu buat file `.env` di root:
   ```
   OPENAQ_API_KEY=isi_key_kamu
   ```
2. Install Docker Desktop (untuk Airflow) dan Python 3.13 (untuk lokal).

### 12.2 Jalur belajar A: lokal, tanpa Docker (disarankan untuk memahami)
```powershell
python -m venv .venv
.venv\Scripts\activate
pip install duckdb==2.0.0 httpx==0.28.1 python-dotenv==1.2.4 dbt-core==1.12.7 dbt-duckdb==1.11.1
pip install -r dashboard/requirements.txt

# 1) Extract + Load
python -m Handler.main

# 2) Transform + test
cd openaq_dbt
dbt build
cd ..

# 3) Lihat hasil
streamlit run dashboard/app.py          # http://localhost:8501
```

### 12.2b Menjalankan test
```powershell
pip install -r requirements.txt         # sudah termasuk pytest + dbt
pytest                                  # unit test Handler/ (tanpa jaringan, DuckDB in-memory)
cd openaq_dbt
dbt source freshness                    # data di raw masih baru?
dbt build --vars '{"as_of_utc": "2026-11-28T00:00:00+00:00"}'   # opsional: waktu acuan tetap
```
- `tests/test_client.py` mengganti transport HTTP dengan `httpx.MockTransport`, jadi paginasi, retry 5xx, dan rate limit 429 bisa dites tanpa memanggil API sungguhan. `time.sleep` di-patch supaya test tidak menunggu.
- `tests/test_loader.py` memakai `connect(":memory:")`, sehingga file `data/openaq.duckdb` tidak tersentuh. Yang dites: upsert idempotent, `_loaded_at` ikut diperbarui saat upsert (regresi bug lama), dan field opsional yang hilang.
- Di GitHub, `.github/workflows/ci.yml` menjalankan `pytest` lalu `dbt build` di DuckDB kosong setiap push ke `main` dan setiap PR.

### 12.3 Jalur B: dengan Airflow di Docker (seperti produksi)
```powershell
docker compose build                    # build image (ulang bila Dockerfile/requirements berubah)
docker compose up airflow-init          # sekali: siapkan metadata DB
docker compose up -d                    # nyalakan semua service
```
1. Buka http://localhost:8089
2. Cari DAG `openaq_ingest`, lalu klik toggle untuk **unpause**.
3. Klik **Trigger** untuk run manual, atau tunggu jadwal harian.
4. Klik tiap task untuk membaca log-nya.
5. Jalankan dashboard di laptop (`streamlit run dashboard/app.py`). Dashboard membaca `data/openaq.duckdb` yang sama, karena folder `data/` di-mount ke container.

Perintah berguna:
```powershell
docker compose ps                       # status container
docker compose logs -f airflow-scheduler
docker compose down                     # matikan (data Postgres tetap di volume)
```

---

## 13. Keterbatasan dan ide pengembangan

Bagian ini sengaja ditulis supaya kamu tahu **apa yang bisa dikembangkan sendiri** sebagai latihan:

| Keterbatasan saat ini | Ide perbaikan |
|---|---|
| Kategori ISPU hanya berdasarkan batas, bukan angka indeks ISPU | Hitung nilai ISPU dengan rumus interpolasi linear dari Permen LHK |
| Request per sensor berjalan berurutan | Paralelkan request HTTP, tapi tetap satu penulis ke DuckDB |
| Hanya PM2.5 | Jadikan `PARAMETER` daftar (pm10, o3, …) |
| Backfill sejarah belum otomatis | Pakai `catchup=True` atau `airflow dags backfill` dengan rentang tanggal |
| Belum ada test untuk DAG Airflow itu sendiri | Tes import DAG (`DagBag`) dan urutan task, dijalankan di image Airflow |
| CI hanya menjalankan dbt di DB kosong, sehingga test data tidak benar-benar diuji | Tambahkan seed/fixture data kecil untuk `raw.*` di CI |
| Secret dev (`JWT_SECRET`, tanpa login) | Untuk deployment sungguhan: secret dari env/secret manager, aktifkan auth |

---

## 14. Glosarium

| Istilah | Arti singkat |
|---|---|
| **API** | Pintu untuk mengambil data dari sistem lain lewat HTTP |
| **Rate limit** | Batas jumlah request per periode waktu |
| **Paginasi** | Data dibagi per halaman, diambil satu per satu |
| **Exponential backoff** | Jeda retry yang makin lama (1, 2, 4, 8 detik …) |
| **ETL / ELT** | Extract-Transform-Load / Extract-Load-Transform (proyek ini ELT) |
| **Raw / Staging / Marts** | Lapisan data: mentah → dirapikan → siap pakai |
| **Idempotent** | Dijalankan berkali-kali hasilnya sama |
| **Upsert** | Update kalau sudah ada, insert kalau belum (`INSERT OR REPLACE`) |
| **Primary key (PK)** | Kolom (atau gabungan kolom) yang unik per baris |
| **Grain** | Apa yang diwakili satu baris (mis. sensor × jam) |
| **Dimension (dim)** | Tabel deskriptif (lokasi, sensor) |
| **Fact (fct)** | Tabel pengukuran atau kejadian (nilai PM2.5) |
| **View vs Table** | View = query tersimpan (dihitung saat dibaca); Table = hasil disimpan fisik |
| **Materialization** | Cara dbt mewujudkan model (view, table, incremental, …) |
| **`source()` / `ref()`** | Cara dbt merujuk tabel eksternal / model dbt lain |
| **Jinja** | Bahasa template di dbt (`{{ }}`, `{% %}`) |
| **Data test** | Pengecekan otomatis atas isi data |
| **Freshness** | Seberapa baru data terakhir dimuat |
| **DAG** | Graf task berurutan tanpa siklus |
| **Task / DAG run** | Satu unit kerja / satu kali eksekusi DAG |
| **Data interval** | Rentang waktu data yang ditangani satu run |
| **XCom** | Nilai kecil yang dikembalikan task dan disimpan Airflow |
| **Catchup / Backfill** | Menjalankan run untuk tanggal-tanggal yang lalu |
| **Executor** | Cara Airflow menjalankan task (Local, Celery, Kubernetes) |
| **Image / Container** | Cetakan / instance yang berjalan (Docker) |
| **Bind mount / Volume** | Folder yang dibagi antara host dan container |
| **Environment variable** | Konfigurasi dari luar kode (`OPENAQ_DB_PATH`, dll) |
| **PM2.5** | Partikel udara berdiameter ≤ 2.5 µm, satuan µg/m³ |
| **ISPU** | Indeks Standar Pencemar Udara (Indonesia) |
