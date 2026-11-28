"""Dashboard belajar: melihat alur pipeline OpenAQ (raw -> staging -> marts) dan hasil akhirnya.

Jalankan dari root proyek:
    streamlit run dashboard/app.py

Dashboard ini HANYA MEMBACA file DuckDB yang diisi oleh pipeline (Airflow / Handler.main + dbt).
Ia tidak mengubah data apa pun.
"""
import os
import warnings
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("OPENAQ_DB_PATH", ROOT / "data" / "openaq.duckdb"))
DBT_MODELS = ROOT / "openaq_dbt" / "models"

st.set_page_config(page_title="OpenAQ Pipeline", layout="wide")


# ---------------------------------------------------------------------------
# Akses database
# ---------------------------------------------------------------------------
# PENTING: DuckDB hanya mengizinkan satu proses penulis. Kalau dashboard membiarkan koneksi
# terbuka terus, task Airflow yang mau menulis akan gagal (file terkunci). Karena itu setiap
# query membuka koneksi read-only, mengambil hasil, lalu langsung menutupnya.
@st.cache_data(ttl=60)  # hasil disimpan 60 detik agar tidak membuka file terus-menerus
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        # .df() milik duckdb 2.0.0 memanggil DataFrame.drop(inplace=True) secara internal, dan
        # pandas 3.1 memberi peringatan untuk itu. Bukan masalah di kode kita; bungkam hanya itu.
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="The inplace keyword", category=Warning)
            return con.execute(sql, list(params)).df()
    finally:
        con.close()


if not DB_PATH.exists():
    st.error(f"File database belum ada: `{DB_PATH}`. Jalankan pipeline dulu (DAG Airflow atau `python -m Handler.main`).")
    st.stop()

try:
    tables = query("""
        select table_schema as schema, table_name as tabel,
               case table_type when 'VIEW' then 'view' else 'table' end as jenis
        from information_schema.tables
        where table_schema in ('raw', 'staging', 'marts')
        order by 1, 2
    """)
except duckdb.IOException:
    st.error("File DuckDB sedang dikunci proses lain (kemungkinan Airflow sedang menulis). Tunggu task selesai lalu refresh.")
    st.stop()

# Hitung jumlah baris tiap tabel, dipakai di diagram dan tabel ringkasan.
tables["baris"] = [
    int(query(f'select count(*) as n from "{s}"."{t}"')["n"][0])
    for s, t in zip(tables["schema"], tables["tabel"])
]
rows = {f"{s}.{t}": n for s, t, n in zip(tables["schema"], tables["tabel"], tables["baris"])}

st.title("OpenAQ PM2.5 Indonesia: dari API sampai dashboard")
st.caption(f"Sumber: `{DB_PATH}`. Tekan **R** untuk refresh setelah pipeline berjalan.")

tab_flow, tab_trace, tab_dash, tab_browse = st.tabs(
    ["1. Alur pipeline", "2. Lacak satu data", "3. Dashboard PM2.5", "4. Jelajah tabel"]
)

# ---------------------------------------------------------------------------
# TAB 1: Diagram alur, dengan jumlah baris asli dari database
# ---------------------------------------------------------------------------
with tab_flow:
    st.subheader("Data mengalir dari kiri ke kanan")

    def node(name: str, label: str) -> str:
        n = rows.get(name, "?")
        return f'"{name}" [label="{label}\\n{n} baris"];'

    dot = f"""
    digraph G {{
      rankdir=LR; node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11];
      api [label="OpenAQ API\\n(internet)", fillcolor="#e0e0e0"];

      subgraph cluster_raw {{ label="RAW\\nloader.py (Airflow task 1 & 2)"; style=filled; fillcolor="#fdecea";
        node [fillcolor="#f8c9c4"];
        {node("raw.locations", "raw.locations")} {node("raw.sensors", "raw.sensors")}
        {node("raw.measurements_hourly", "raw.measurements_hourly")}
      }}
      subgraph cluster_stg {{ label="STAGING (view)\\ndbt: rapikan saja"; style=filled; fillcolor="#fff6e0";
        node [fillcolor="#fde4a8"];
        {node("staging.stg_openaq__locations", "stg_locations")} {node("staging.stg_openaq__sensors", "stg_sensors")}
        {node("staging.stg_openaq__measurements_hourly", "stg_measurements_hourly")}
      }}
      subgraph cluster_marts {{ label="MARTS (table)\\ndbt: logika bisnis (Airflow task 3)"; style=filled; fillcolor="#e6f4ea";
        node [fillcolor="#b9e1c4"];
        {node("marts.dim_locations", "dim_locations")} {node("marts.dim_sensors", "dim_sensors")}
        {node("marts.fct_pm25_hourly", "fct_pm25_hourly")} {node("marts.fct_pm25_daily", "fct_pm25_daily\\n+ kategori ISPU")}
      }}
      dash [label="Dashboard ini", fillcolor="#c9dcf8"];

      api -> "raw.locations"; api -> "raw.sensors"; api -> "raw.measurements_hourly";
      "raw.locations" -> "staging.stg_openaq__locations";
      "raw.sensors" -> "staging.stg_openaq__sensors";
      "raw.measurements_hourly" -> "staging.stg_openaq__measurements_hourly";
      "staging.stg_openaq__locations" -> "marts.dim_locations";
      "staging.stg_openaq__locations" -> "marts.dim_sensors";
      "staging.stg_openaq__sensors" -> "marts.dim_sensors";
      "staging.stg_openaq__measurements_hourly" -> "marts.fct_pm25_hourly";
      "staging.stg_openaq__sensors" -> "marts.fct_pm25_hourly";
      "marts.dim_locations" -> "marts.fct_pm25_hourly";
      "marts.fct_pm25_hourly" -> "marts.fct_pm25_daily";
      "marts.fct_pm25_daily" -> dash; "marts.dim_locations" -> dash;
    }}
    """
    st.graphviz_chart(dot, width="stretch")

    c1, c2, c3 = st.columns(3)
    c1.markdown("**RAW**: persis seperti dari API. Tidak diubah, supaya bisa diolah ulang tanpa download lagi.")
    c2.markdown("**STAGING**: ganti nama kolom, trim teks, samakan huruf besar/kecil. Belum ada logika bisnis.")
    c3.markdown("**MARTS**: filter, join, agregasi, kategori ISPU. Tabel inilah yang dipakai analis atau dashboard.")

    st.subheader("Kenapa jumlah baris berkurang di fct_pm25_hourly?")
    drop = query("""
        select
            count(*)                                                        as total_staging,
            count(*) filter (where m.parameter_name <> 'pm25')              as bukan_pm25,
            count(*) filter (where m.value is null)                         as nilai_kosong,
            count(*) filter (where not coalesce(l.is_within_indonesia, false)) as di_luar_indonesia
        from staging.stg_openaq__measurements_hourly m
        left join staging.stg_openaq__sensors s using (sensor_id)
        left join marts.dim_locations l using (location_id)
    """).iloc[0]
    d1, d2, d3, d4, d5 = st.columns(5)
    d1.metric("Baris di staging", int(drop.total_staging))
    d2.metric("Bukan PM2.5", int(drop.bukan_pm25))
    d3.metric("Nilai kosong", int(drop.nilai_kosong))
    d4.metric("Lokasi di luar Indonesia", int(drop.di_luar_indonesia))
    d5.metric("Lolos ke fct_pm25_hourly", rows.get("marts.fct_pm25_hourly", 0))
    st.caption("Ketiga alasan bisa tumpang tindih. Lihat klausa `where` di `fct_pm25_hourly.sql`.")

    st.dataframe(tables, hide_index=True, width="stretch")

# ---------------------------------------------------------------------------
# TAB 2: Ikuti SATU pengukuran dari raw -> staging -> marts
# ---------------------------------------------------------------------------
with tab_trace:
    st.subheader("Ikuti satu pengukuran melewati setiap lapisan")
    sensors = query("""
        select h.sensor_id, l.location_name, count(*) as n
        from marts.fct_pm25_hourly h join marts.dim_locations l using (location_id)
        group by all order by n desc
    """)
    if sensors.empty:
        st.info("Belum ada data di fct_pm25_hourly.")
    else:
        label = {r.sensor_id: f"{r.sensor_id} - {r.location_name} ({r.n} jam)" for r in sensors.itertuples()}
        sid = st.selectbox("Sensor", sensors["sensor_id"], format_func=label.get)
        hours = query(
            "select measured_at_utc from marts.fct_pm25_hourly where sensor_id = ? order by 1 desc",
            (int(sid),),
        )["measured_at_utc"]
        ts = st.selectbox("Jam (UTC)", hours, format_func=lambda t: t.strftime("%Y-%m-%d %H:%M"))
        params = (int(sid), ts.to_pydatetime())

        st.markdown("#### 1) RAW: `raw.measurements_hourly` (ditulis oleh `Handler/loader.py`)")
        raw = query("""
            select sensor_id, datetime_from_utc, value, parameter_name, units, payload, _loaded_at
            from raw.measurements_hourly
            where sensor_id = ? and (datetime_from_utc at time zone 'UTC') = ?
        """, params)
        st.dataframe(raw.drop(columns="payload"), hide_index=True, width="stretch")
        with st.expander("Lihat JSON asli dari API (kolom payload)"):
            if not raw.empty:
                st.json(raw["payload"][0])

        st.markdown("#### 2) STAGING: `stg_openaq__measurements_hourly` (view dbt)")
        st.dataframe(query("""
            select * from staging.stg_openaq__measurements_hourly
            where sensor_id = ? and measured_at_utc = ?
        """, params), hide_index=True, width="stretch")
        with st.expander("SQL dbt-nya"):
            st.code((DBT_MODELS / "staging" / "stg_openaq__measurements_hourly.sql").read_text(), language="sql")

        st.markdown("#### 3) MARTS: `fct_pm25_hourly` (sudah di-join dengan lokasi, waktu lokal)")
        hourly = query("""
            select * from marts.fct_pm25_hourly where sensor_id = ? and measured_at_utc = ?
        """, params)
        st.dataframe(hourly, hide_index=True, width="stretch")
        with st.expander("SQL dbt-nya"):
            st.code((DBT_MODELS / "marts" / "fct_pm25_hourly.sql").read_text(), language="sql")

        st.markdown("#### 4) MARTS: `fct_pm25_daily` (jam ini ikut dirata-ratakan ke hari lokalnya)")
        if not hourly.empty:
            st.dataframe(query("""
                select * from marts.fct_pm25_daily
                where location_id = ? and date_local = cast(? as date)
            """, (int(hourly["location_id"][0]), hourly["measured_at_local"][0].to_pydatetime())),
                hide_index=True, width="stretch")
        with st.expander("SQL dbt-nya"):
            st.code((DBT_MODELS / "marts" / "fct_pm25_daily.sql").read_text(), language="sql")

# ---------------------------------------------------------------------------
# TAB 3: Dashboard hasil akhir (hanya membaca tabel MARTS)
# ---------------------------------------------------------------------------
ISPU_COLORS = {
    "Baik": "#2e7d32", "Sedang": "#1565c0", "Tidak Sehat": "#f9a825",
    "Sangat Tidak Sehat": "#c62828", "Berbahaya": "#212121",
}

with tab_dash:
    daily = query("""
        select d.*, l.location_name
        from marts.fct_pm25_daily d join marts.dim_locations l using (location_id)
    """)
    locs = query("select * from marts.dim_locations where is_within_indonesia")

    if daily.empty:
        st.info("Belum ada data di fct_pm25_daily.")
    else:
        daily["date_local"] = pd.to_datetime(daily["date_local"])
        max_d = daily["date_local"].max()
        picked = st.date_input(
            "Rentang tanggal",
            value=(max_d - pd.Timedelta(days=30), max_d),
            min_value=daily["date_local"].min(), max_value=max_d,
        )
        # Saat user baru memilih tanggal awal, Streamlit hanya mengembalikan 1 tanggal.
        start, end = (picked[0], picked[-1]) if isinstance(picked, tuple) and picked else (picked, picked)
        f = daily[(daily["date_local"] >= pd.Timestamp(start)) & (daily["date_local"] <= pd.Timestamp(end))]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Lokasi dengan data", f["location_id"].nunique())
        k2.metric("Rata-rata PM2.5", f"{f['pm25_avg'].mean():.1f} µg/m³" if len(f) else "-")
        k3.metric("Hari 'Tidak Sehat' atau lebih buruk",
                  int(f["ispu_category"].isin(["Tidak Sehat", "Sangat Tidak Sehat", "Berbahaya"]).sum()))
        k4.metric("Lokasi aktif (7 hari)", int(locs["is_active"].fillna(False).sum()))

        left, right = st.columns([3, 2])
        with left:
            st.markdown("**Rata-rata harian PM2.5 per lokasi**")
            top = f.groupby("location_name")["n_hours"].sum().nlargest(15).index.tolist()
            pick = st.multiselect("Lokasi", sorted(f["location_name"].unique()), default=top[:5])
            st.line_chart(f[f["location_name"].isin(pick)], x="date_local", y="pm25_avg",
                          color="location_name", y_label="PM2.5 (µg/m³)", x_label="Tanggal")
            st.caption("Batas ISPU PM2.5 (24 jam): Baik ≤15.5, Sedang ≤55.4, Tidak Sehat ≤150.4, Sangat Tidak Sehat ≤250.4.")
        with right:
            st.markdown("**Jumlah lokasi-hari per kategori ISPU**")
            cat = (f["ispu_category"].value_counts()
                   .reindex(ISPU_COLORS.keys(), fill_value=0).rename_axis("kategori").reset_index(name="jumlah"))
            st.bar_chart(cat, x="kategori", y="jumlah", horizontal=True, sort=False)

        st.markdown("**Peta stasiun** (hijau = aktif 7 hari terakhir, abu-abu = tidak aktif)")
        # is_active bisa NULL (lokasi tanpa last_seen), anggap tidak aktif.
        active = locs["is_active"].fillna(False).astype(bool)
        locs["color"] = active.map({True: "#2e7d32", False: "#9e9e9e"})
        st.map(locs, latitude="latitude", longitude="longitude", color="color", size=3000)

        st.markdown("**Hari terakhir per lokasi**")
        latest = (f.sort_values("date_local").groupby("location_id").tail(1)
                  .sort_values("pm25_avg", ascending=False))
        st.dataframe(
            latest[["location_name", "date_local", "pm25_avg", "pm25_max", "n_hours", "is_complete_day", "ispu_category"]],
            hide_index=True, width="stretch",
            column_config={"date_local": st.column_config.DateColumn("tanggal"),
                           "pm25_avg": st.column_config.NumberColumn("PM2.5 rata-rata", format="%.1f")},
        )

# ---------------------------------------------------------------------------
# TAB 4: Lihat isi tabel apa pun
# ---------------------------------------------------------------------------
with tab_browse:
    st.subheader("Intip isi tabel")
    choice = st.selectbox("Tabel", list(rows), format_func=lambda k: f"{k}  ({rows[k]} baris)")
    s, t = choice.split(".")
    n = st.slider("Jumlah baris", 10, 500, 50)
    st.dataframe(query(f'select * from "{s}"."{t}" limit {int(n)}'), width="stretch")
    st.markdown("Atau tulis SQL sendiri (read-only):")
    sql = st.text_area("SQL", "select ispu_category, count(*) as n\nfrom marts.fct_pm25_daily\ngroup by 1\norder by 2 desc")
    if st.button("Jalankan"):
        try:
            st.dataframe(query(sql), width="stretch")
        except duckdb.Error as e:
            st.error(str(e))
