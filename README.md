# Subaraya · Dashboard Kota

Dashboard analitik statis untuk kota simulasi **Subaraya** — 4 zona yang
dipantau dari tiga sisi: **kemacetan lalu lintas**, **kualitas udara**, dan
**keselamatan (kecelakaan)**.

Dibuat dengan Vite + React + TypeScript, Tailwind CSS v4, komponen bergaya
shadcn/ui, Recharts, dan react-leaflet. Tidak ada backend: seluruh angka dibaca
dari satu berkas JSON statis.

---

## Menjalankan

```bash
npm install
npm run dev
```

Buka <http://localhost:5173>.

Perintah lain:

| Perintah            | Kegunaan                                      |
| ------------------- | --------------------------------------------- |
| `npm run dev`       | Server pengembangan (hot reload)              |
| `npm run build`     | Build produksi ke `dist/`                     |
| `npm run preview`   | Pratinjau hasil build                         |
| `npm run typecheck` | Cek tipe TypeScript tanpa membangun           |

---

## Isi halaman

| Rute            | Halaman        | Isi utama                                                                      |
| --------------- | -------------- | ------------------------------------------------------------------------------ |
| `/`             | Overview       | 4 KPI + sparkline, donut kepadatan, tren PM2.5, radar antar zona, callout zona prioritas |
| `/kemacetan`    | Kemacetan      | KPI kecepatan & volume, donut kepadatan, kecepatan per jam, kecepatan vs volume per zona |
| `/udara`        | Kualitas Udara | KPI PM2.5, bar PM2.5 per zona (warna ikut kategori), donut kategori jam, tren bulanan |
| `/keselamatan`  | Keselamatan    | KPI kecelakaan & korban, kecelakaan per jam, kecelakaan & korban per zona      |
| `/prediksi`     | Prediksi       | Perkiraan kecelakaan 24 jam & 7 hari ke depan dari model machine learning      |
| `/peta`         | Peta Zona      | Peta OpenStreetMap, 4 penanda lingkaran, pemilih metrik, peringkat zona        |
| `/data`         | Data           | Tabel per zona: cari, urutkan, paginasi, badge status, unduh CSV               |

### Interaksi

- **Filter global** (di top bar): pilih zona (Semua / Z01–Z04) dan rentang waktu
  (6 / 12 / 24 bulan / seluruh periode). Pilihan disimpan di `localStorage`.
- **Mode gelap/terang**: tombol matahari–bulan di kanan atas membuka menu
  **Terang / Gelap / Ikuti sistem**. Pilihan tersimpan di `localStorage`, dan
  sebuah skrip kecil di `index.html` menerapkannya sebelum React jalan supaya
  tidak ada kedipan warna saat halaman dimuat.
- **Pencarian** (`/` untuk fokus): mencari halaman dan zona. Memilih zona akan
  menerapkannya sebagai filter global.
- **Peta**: pemilih metrik (PM2.5 / Kecepatan / Kecelakaan) mengubah warna dan
  ukuran penanda; memilih zona pada filter global akan menggeser peta ke zona itu.
- **Tabel**: klik judul kolom untuk mengurutkan, ada kotak pencarian dan paginasi.

Semuanya responsif sampai lebar ponsel (sidebar berubah menjadi drawer),
menghormati `prefers-reduced-motion`, dan bisa dinavigasi dengan keyboard.

---

## Mengganti data contoh dengan data asli

Berkas `src/data/dashboard.json` saat ini berisi **DATA CONTOH**. Aplikasi hanya
membaca berkas ini — tidak ada tempat lain yang perlu diubah.

### 1. Siapkan CSV

Letakkan hasil ekspor PostgreSQL di folder `data_raw/` (folder ini sudah
diabaikan oleh git):

```
data_raw/
  mart_hourly.csv
  dim_zone.csv
```

`mart_hourly.csv` — satu baris per (zona, jam):

| Kolom              | Wajib | Keterangan                                |
| ------------------ | ----- | ----------------------------------------- |
| `ts`               | ✔     | Waktu per jam (apa pun yang bisa diparse)  |
| `zone_id`          | ✔     | Kode zona, mis. `Z01`                      |
| `speed_kmh`        |       | Kecepatan rata-rata (km/jam), boleh kosong |
| `volume`           |       | Volume kendaraan per jam, boleh kosong     |
| `congestion_level` |       | `LOW` / `MEDIUM` / `HIGH`                  |
| `pm25`             |       | Konsentrasi PM2.5 (µg/m³)                  |
| `accidents`        |       | Jumlah kecelakaan pada jam itu             |
| `casualties`       |       | Jumlah korban pada jam itu                 |

`dim_zone.csv` — satu baris per zona:

| Kolom         | Wajib | Keterangan                    |
| ------------- | ----- | ----------------------------- |
| `zone_id`     | ✔     | Kode zona                      |
| `zone_name`   |       | Nama zona untuk ditampilkan    |
| `lat`, `lng`  |       | Koordinat penanda peta         |
| `capacity`    |       | Kapasitas ruas (kendaraan/jam) |
| `speed_limit` |       | Batas kecepatan (km/jam)       |

Nama kolom tidak harus persis: skrip mengenali beberapa alias (mis. `avg_speed`,
`kecepatan`, `pm2_5`, `kode_zona`). Kalau nama kolommu belum dikenali, tambahkan
ke `HOURLY_ALIASES` / `ZONE_ALIASES` di bagian atas `scripts/aggregate.py`.

### 2. Jalankan skrip agregasi

```bash
pip install pandas
python scripts/aggregate.py
```

Atau tunjuk lokasinya sendiri:

```bash
python scripts/aggregate.py --hourly data_raw/mart_hourly.csv --zones data_raw/dim_zone.csv --sep ";"
```

Skrip menulis ulang `src/data/dashboard.json` lalu mencetak ringkasannya. Setelah
itu cukup muat ulang halaman `npm run dev` — tidak ada langkah lain.

**Aturan agregasi yang dipakai**

- Rata-rata (kecepatan, volume, PM2.5) **mengabaikan nilai kosong** — banyak baris
  lalu lintas memang `NULL`.
- Kecelakaan dan korban **dijumlahkan**; `NULL` dihitung sebagai 0.
- `pm25_by_month` dikelompokkan per `YYYY-MM`.
- `acc_by_hour` memakai penjumlahan, `speed_by_hour` memakai rata-rata.
- Bila `congestion_level` tidak ada di CSV, skrip menurunkannya dari rasio
  volume/kapasitas (<0,50 `LOW`, <0,80 `MEDIUM`, sisanya `HIGH`) dan memberi
  peringatan di terminal serta catatan pada `meta.note`. Matikan dengan
  `--no-derive-congestion`.

### 3. (Opsional) buat ulang data contoh

```bash
node scripts/generate-mock.mjs src/data/dashboard.json
```

---

## Halaman Prediksi (model machine learning)

Halaman `/prediksi` menampilkan keluaran tiga model dari data scientist:

| Model | Memprediksi |
| ----- | ----------- |
| `expected_accident_pipeline.joblib` | perkiraan **jumlah** kecelakaan pada satu jam |
| `accident_occurrence_pipeline.joblib` | **peluang** ada kecelakaan pada jam itu |
| `severe_accident_risk_pipeline.joblib` | **peluang** kecelakaan tergolong berat |

### Kenapa model tidak dijalankan di dalam web

Dashboard ini situs statis — tidak ada Python di sisi server, dan browser tidak
bisa menjalankan scikit-learn. Untungnya tidak perlu: ketiga model hanya memakai
fitur kalender dan kode zona sebagai masukan

```
zone_id, hour, day_of_week, day_of_month, month, is_weekend
```

sehingga keluarannya deterministik dan seluruh jawabannya bisa dihitung sekali
di muka. `scripts/predict.py` menjalankan model, lalu menyimpan hasilnya sebagai
tabel di `src/data/predictions.json`.

Hasilnya: **473 MB berkas model menjadi 358 KB JSON**, tanpa perbedaan angka
sama sekali dibanding memanggil model secara langsung.

### Memperbarui prediksi

Letakkan berkas `.joblib` di folder `models/` (folder ini diabaikan git karena
ukurannya), lalu:

```bash
pip install scikit-learn pandas joblib
python scripts/predict.py
```

Pilihan lain:

```bash
python scripts/predict.py --models "D:/model" --start 2027-01-01 --days 240
```

Default-nya menghitung 240 hari ke depan mulai hari ini. Kalau tanggal yang
dibuka pengguna berada di luar rentang itu, halaman Prediksi menampilkan
peringatan bahwa berkasnya perlu diperbarui — bukan diam-diam menampilkan
angka yang salah.

### Kelas `AccidentFeatureEngineer`

Ketiga `.joblib` memanggil kelas custom bernama `AccidentFeatureEngineer`, tapi
pickle hanya menyimpan namanya — bukan kodenya. Tanpa definisi kelas itu,
`joblib.load()` gagal dengan `AttributeError`.

`scripts/predict.py` sudah memuat rekonstruksi kelas tersebut. Isinya
disimpulkan dari dua bukti di dalam model sendiri: daftar kolom pada
`ColumnTransformer`, dan statistik `StandardScaler` yang cocok dengan kalender
per jam biasa (selisih terbesar 0,005). Kalau data scientist mengirim kelas
aslinya, ganti bagian itu dengan versi mereka lalu jalankan ulang skripnya.

### Angka evaluasi model

Kalau folder model berisi `metrics.json` (dihasilkan `ml/train.py`),
`scripts/predict.py` menyalinnya ke `predictions.json` dan halaman Prediksi
menampilkannya bersama pembanding tebakan naif.

Skrip juga membandingkan estimator yang **dievaluasi** dengan estimator yang
benar-benar **dipakai** untuk membuat prediksi. Kalau berbeda — misalnya berkas
`.joblib` berasal dari percobaan lain — perbedaannya ditandai, dan halaman
menyatakan bahwa angka itu belum menggambarkan model yang sedang tampil.
Tanpa `metrics.json`, halaman menyatakan modelnya belum punya angka evaluasi.

### Catatan keterbatasan dibuat otomatis

`scripts/predict.py` juga memeriksa modelnya dan menyimpan hasilnya di
`meta.diagnostics`. Halaman Prediksi membaca bagian itu untuk menampilkan
catatan keterbatasan — misalnya ketika kurva 24 jam ternyata monoton, atau
ketika model jumlah dan model peluang saling bertentangan.

Tidak ada peringatan yang ditulis permanen di kode React: begitu modelnya
diperbaiki dan skripnya dijalankan ulang, catatan itu hilang dengan sendirinya.

---

## Kontrak data

Bentuk lengkap `dashboard.json` didefinisikan di [`src/types.ts`](src/types.ts).

```jsonc
{
  "kpi":        { "avg_speed": 27.0, "avg_pm25": 77.6, "total_accidents": 378531,
                  "total_casualties": 481904, "pct_high": 25.7,
                  "date_start": "Dec 2021", "date_end": "Sep 2025" },
  "congestion": { "LOW": 37730, "MEDIUM": 23468, "HIGH": 21126 },
  "per_zone":   [ { "zone": "Z01", "name": "Pusat Kota", "speed": 29.7, "vol": 3276,
                    "pm25": 81.4, "acc": 94955, "cas": 120862 } ],
  "speed_by_hour": [ /* 24 angka, jam 0..23 */ ],
  "acc_by_hour":   [ /* 24 angka, jam 0..23 */ ],
  "pm25_by_month": { "labels": ["2021-12", "..."], "values": [5.7, "..."] },
  "pm25_cat":   { "Baik": 22232, "Sedang": 41718,
                  "Tidak Sehat": 46570, "Sangat Tidak Sehat": 17883 },
  "zones":      [ { "zone": "Z01", "name": "Pusat Kota", "lat": -7.2575, "lng": 112.7521,
                    "cap": 6000, "limit": 50, "pm25": 81.4, "speed": 29.7, "acc": 94955 } ]
}
```

### Bagian opsional

Delapan kunci berikut **tidak wajib**. Kalau ada, filter global jadi jauh lebih
berguna; kalau tidak ada, dashboard tetap jalan dan otomatis menampilkan angka
se-kota beserta catatan kecil bahwa chart tersebut adalah agregat.

| Kunci                 | Gunanya                                                     |
| --------------------- | ----------------------------------------------------------- |
| `vol_by_hour`         | Profil volume harian (sparkline KPI volume)                 |
| `speed_by_month`      | Sparkline & delta KPI kecepatan                             |
| `acc_by_month`        | Sparkline & delta KPI kecelakaan                            |
| `cas_by_month`        | Sparkline & delta KPI korban                                |
| `high_share_by_month` | Sparkline & delta KPI “% waktu macet”                       |
| `by_zone`             | Semua seri di atas, dipecah per kode zona — membuat filter zona memengaruhi chart per jam, donut, dan tren bulanan |
| `meta`                | Jejak proses agregasi (waktu, sumber, jumlah baris)         |

`scripts/aggregate.py` selalu mengisi semuanya.

### Kategori PM2.5 (µg/m³)

| Kategori           | Rentang   |
| ------------------ | --------- |
| Baik               | ≤ 15      |
| Sedang             | 16 – 55   |
| Tidak Sehat        | 56 – 150  |
| Sangat Tidak Sehat | > 150     |

Badge status pada tabel memakai ambang yang sama: **Aman** ≤ 55,
**Waspada** ≤ 150, **Bahaya** > 150. Ambangnya ada di satu tempat —
`PM25_THRESHOLDS` di [`src/lib/colors.ts`](src/lib/colors.ts) — dan disalin ke
`scripts/aggregate.py` (konstanta `PM25_*`). Ubah keduanya bila perlu.

---

## Catatan kebenaran data

Hal-hal berikut sengaja dijaga di dalam kode maupun tampilan:

1. **Tiga domain, tiga sumber.** Lalu lintas, kualitas udara, dan kecelakaan
   berasal dari sumber data yang berbeda. Dashboard tidak pernah menyandingkan
   keduanya sebagai hubungan sebab-akibat, dan tidak ada chart korelasi antar
   domain. Halaman Overview menampilkan disclaimer ini.
2. **Lokasi & waktu adalah simulasi** — data publik yang dinormalisasi ke dalam
   4 zona.
3. **Angka kecelakaan adalah total kumulatif** sepanjang periode, bukan kejadian
   per bulan. Labelnya selalu menulis “total tercatat”.
4. **Pola per jam ditampilkan apa adanya.** Tidak ada klaim bahwa jam tertentu
   menyebabkan kemacetan atau kecelakaan.
5. **Chart yang tidak ikut terfilter memberi tahu pembaca.** Profil per jam dan
   distribusi kategori dihitung dari seluruh periode; catatan kaki setiap chart
   menyebutkan hal itu, begitu juga bila rincian per zona tidak tersedia
   sehingga yang tampil adalah agregat se-kota.
6. **Skor radar bersifat relatif.** Nilainya 0–100 dibanding zona lain
   (kelancaran = rasio terhadap kecepatan tertinggi; udara bersih dan
   keselamatan = rasio terhadap nilai terendah), bukan skor mutlak. Skor
   gabungan hanya meringkas tiga indikator mandiri.
7. **Rata-rata pada rentang waktu yang dipersempit** dihitung sebagai rata-rata
   dari rata-rata bulanan (tiap bulan berbobot sama). Untuk cakupan jam yang
   mirip tiap bulan, selisihnya dapat diabaikan. Pada “Seluruh periode”, yang
   dipakai adalah angka agregat yang persis.
8. **Halaman Prediksi jelas-jelas berisi ramalan, bukan kejadian.** Angkanya
   keluaran model statistik, diberi disclaimer di bagian atas halaman, dan
   keterbatasan modelnya ditampilkan apa adanya — termasuk ketika model jumlah
   dan model peluang saling bertentangan. Ketiga model hanya memakai waktu dan
   kode zona, jadi tidak mencampur domain satu sama lain.

---

## Struktur folder

```
models/                   # berkas .joblib dari data scientist (diabaikan git)
scripts/
  aggregate.py            # CSV asli  -> src/data/dashboard.json  (pandas)
  predict.py              # model .joblib -> src/data/predictions.json (sklearn)
  generate-mock.mjs       # Pembuat data contoh (Node, tanpa dependensi)
src/
  components/
    ui/                   # Primitif bergaya shadcn (button, card, select, ...)
    layout/               # AppLayout, Sidebar, TopBar
    ChartCard.tsx         # Pembungkus seragam untuk semua chart
    KpiCard.tsx           # Kartu KPI: angka besar + delta + sparkline
    Sparkline.tsx         # Mini chart tanpa sumbu
    ChartTooltip.tsx      # Tooltip seragam, ikut tema
    DataTable.tsx         # Tabel: cari + urutkan + paginasi
    ZoneMap.tsx           # Peta Leaflet + penanda lingkaran
    ZoneFilter.tsx        # Select zona & rentang waktu
    ThemeToggle.tsx       # Tombol gelap/terang
    GlobalSearch.tsx      # Pencarian halaman & zona
    charts/DonutChart.tsx # Donut + legenda bernilai
  pages/                  # Satu berkas per rute
  store/
    theme.tsx             # Konteks tema (localStorage)
    filters.tsx           # Konteks filter global (localStorage)
  hooks/                  # use-dashboard, use-chart-colors, use-media-query, ...
  lib/
    metrics.ts            # Data + filter  ->  angka siap pakai
    forecast.ts           # Pembacaan tabel prediksi model
    colors.ts             # Palet, ambang PM2.5, status zona
    format.ts             # Format angka & tanggal locale id-ID
    notes.ts              # Teks catatan kaki tentang cakupan data
    nav.ts                # Daftar menu (dipakai sidebar & pencarian)
    utils.ts              # cn(), mean(), sum(), clamp()
  data/
    dashboard.json        # <- sumber angka historis
    predictions.json      # <- hasil model, dibuat scripts/predict.py
    index.ts
  types.ts                # Kontrak data
  index.css               # Design token (CSS variable) + tema Leaflet
```

---

## Warna

Semua warna berasal dari CSS variable di `src/index.css` sehingga mode gelap dan
terang sama-sama punya kontras yang cukup.

| Peran                | Terang    | Gelap     |
| -------------------- | --------- | --------- |
| Aksen utama (teal)   | `#10b981` | `#34d399` |
| Aksen kedua (coral)  | `#f97316` | `#fb923c` |
| Pilar Kemacetan      | `#3b5bdb` | `#7c93f5` |
| Kondisi baik         | `#2f9e44` | `#51cf66` |
| Kondisi waspada      | `#f08c00` | `#ffc078` |
| Kondisi bahaya       | `#e03131` | `#ff6b6b` |

Recharts dan Leaflet butuh nilai hex (bukan `var(--x)`) untuk menghitung gradien
dan opacity, jadi nilai yang sama juga ada di `PALETTE` pada
`src/lib/colors.ts`. **Kalau mengganti warna, ubah di kedua berkas itu.**

---

## Menambah komponen shadcn/ui

Proyek ini sudah punya `components.json`, jadi komponen baru bisa ditambahkan
seperti biasa:

```bash
npx shadcn@latest add tooltip
```

---

# Subaraya batch dan streaming ETL

Proyek ini menggunakan data lalu lintas, kualitas udara, dan kecelakaan untuk
mensimulasikan sistem analitik transportasi Kota Subaraya.

Nama teknis seperti `metropolia_warehouse`, `metropolia.events`, dan
`metropolia_daily_batch` tetap digunakan di dalam kode untuk menjaga
kompatibilitas dengan konfigurasi yang sudah dijalankan.

## Periode simulasi

- Historical backfill: 1 Januari 2022 sampai 31 Agustus 2025.
- Daily batch demo: 1 sampai 7 September 2025.
- Streaming cutover: mulai 8 September 2025.

Data traffic dan kualitas udara dari 1 Maret 2013 sampai 28 Februari 2017
digeser ke periode simulasi 2022 sampai 2025. Data kecelakaan menggunakan
tahun 2022 sampai 2025 tanpa pergeseran tahun.

Lokasi sumber dipetakan menjadi empat zona sintetis:

```text
Z01
Z02
Z03
Z04
```

## Arsitektur

### ETL batch

```text
Raw data
    -> Airflow
    -> Validasi input
    -> PySpark batch
    -> Staging PostgreSQL
    -> UPSERT fact
    -> Mart
```

Airflow menjalankan validasi, transformasi PySpark, dan pemuatan ke PostgreSQL.
Data staging ditulis ulang pada setiap batch. Data fact dan mart menggunakan
UPSERT agar proses dapat dijalankan ulang tanpa menggandakan baris.

### ETL streaming

```text
Manual/automatic producer
    -> Apache Kafka
    -> Spark Structured Streaming
    -> UPSERT fact
    -> Mart
```

Producer membentuk pesan JSON dan mengirimkannya ke topic Kafka
`metropolia.events`. Consumer PySpark membaca event dalam micro-batch,
memvalidasi dan mentransformasi data, lalu memperbarui fact table dan
`mart_metropolia_hourly`.

Streaming tidak menggunakan staging table batch.

## Struktur proyek

```text
metropolia-etl/
├── dags/
│   └── metropolia_pipeline_dag.py
├── data/
│   ├── raw/
│   ├── reference/
│   ├── batch/
│   ├── canonical/
│   ├── quarantine/
│   └── stream/
├── models/
├── scripts/
│   ├── check_input.py
│   └── spark_metropolia_etl.py
├── sql/
│   ├── init.sql
│   ├── init_streaming.sql
│   └── upsert_staging.sql
├── streaming/
│   ├── manual_producer.py
│   ├── automatic_producer.py
│   ├── spark_streaming_metropolia.py
│   ├── Dockerfile.producer
│   └── Dockerfile.spark
├── tests/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── README.md
└── STREAMING_DEMO.md
```

## Fungsi komponen

- `data/raw`: dataset sumber sebelum transformasi.
- `data/reference`: kalender simulasi, periode pipeline, dan pemetaan zona.
- `scripts/check_input.py`: memeriksa file dan kolom yang dibutuhkan.
- `scripts/spark_metropolia_etl.py`: menjalankan ETL batch menggunakan PySpark.
- `dags/metropolia_pipeline_dag.py`: mendefinisikan DAG backfill dan daily batch.
- `sql/init.sql`: membuat warehouse, fact table, mart, dan modeling view.
- `sql/upsert_staging.sql`: memindahkan data staging ke fact dan mart.
- `sql/init_streaming.sql`: membuat tabel audit streaming.
- `streaming/manual_producer.py`: mengirim satu event secara manual.
- `streaming/automatic_producer.py`: menyimulasikan sensor secara periodik.
- `streaming/spark_streaming_metropolia.py`: consumer dan transformasi streaming.
- `models`: area kerja modeling yang dikelola anggota data science.
- `STREAMING_DEMO.md`: petunjuk lengkap demonstrasi ETL streaming.

## Prasyarat

1. Docker Desktop dengan Docker Compose.
2. Minimal sekitar 8 GB RAM tersedia untuk Docker.
3. Port 5432, 8080, dan 9092 tidak digunakan aplikasi lain.
4. Internet pada build pertama untuk mengunduh image dan dependency.

## Persiapan setelah clone

Buka CMD atau PowerShell pada folder repository.

Buat `.env` dari contoh konfigurasi:

```cmd
copy .env.example .env
```

File `.env` tidak diunggah ke GitHub.

Jika port tertentu sudah digunakan, ubah nilainya di `.env`.

## Validasi data input

Validasi dapat dijalankan sebelum Docker:

```cmd
python scripts/check_input.py --data-root data
```

Hasil yang benar memiliki:

```text
status             : OK
traffic_files      : 1
air_quality_files  : 12
accident_files     : 4
```

## Menjalankan seluruh infrastruktur

```cmd
docker compose up -d --build
```

Periksa status container:

```cmd
docker compose ps
```

Build pertama dapat berlangsung lebih lama karena Docker mengunduh image,
package Python, dan JAR Spark.

## Akses Airflow

Buka:

```text
http://localhost:8080
```

Gunakan:

```text
Username: admin
Password: admin
```

DAG yang tersedia:

```text
metropolia_historical_backfill
metropolia_daily_batch
```

## Menjalankan historical backfill

Historical backfill dijalankan satu kali melalui Airflow:

1. Buka DAG `metropolia_historical_backfill`.
2. Aktifkan DAG jika masih paused.
3. Tekan `Trigger DAG`.
4. Tunggu task berikut selesai:

```text
validate_backfill_inputs
    -> transform_historical_data
    -> upsert_historical_warehouse
```

Backfill memproses periode 1 Januari 2022 sampai 31 Agustus 2025.

## Daily batch

Daily batch menggunakan alur:

```text
validate_daily_inputs
    -> transform_daily_data
    -> upsert_daily_warehouse
```

Pada pelaksanaan tugas, DAG dijadwalkan pukul 14.00 WIB dan memetakan tanggal
eksekusi aktual ke tanggal simulasi 1 sampai 7 September 2025.

Konfigurasi runtime pada DAG menggunakan tanggal tetap. Oleh karena itu, clone
baru yang dijalankan setelah periode demonstrasi tidak otomatis membuat ulang
tujuh scheduled run. Untuk laporan dan presentasi, gunakan hasil eksekusi yang
sudah direkam. Jika jadwal perlu disimulasikan kembali, sesuaikan nilai berikut
di `dags/metropolia_pipeline_dag.py`:

```python
DAILY_RUNTIME_FIRST_DATE
start_date
end_date
```

Tanggal data simulasi tetap dimulai dari 1 September 2025.

## Koneksi PostgreSQL melalui DBeaver

```text
Connection name : Subaraya Warehouse
Host            : localhost
Port            : 5432
Database        : metropolia_warehouse
Username        : de_user
Password        : password123
```

Koneksi internal Airflow menggunakan host:

```text
target-postgres
```

Host tersebut hanya digunakan antar-container. DBeaver tetap menggunakan
`localhost`.

## Pemeriksaan hasil batch

```sql
SELECT *
FROM etl_batch_log
ORDER BY processed_at DESC;
```

```sql
SELECT
    MIN(event_hour) AS first_event,
    MAX(event_hour) AS latest_event,
    COUNT(*) AS total_rows
FROM mart_metropolia_hourly;
```

```sql
SELECT
    event_date,
    zone_id,
    traffic_volume_total,
    average_pm25,
    accident_count
FROM mart_metropolia_daily
ORDER BY event_date DESC, zone_id
LIMIT 20;
```

## Menjalankan ETL streaming

Jalankan database, Kafka, tabel audit, dan consumer:

```cmd
docker compose up -d target-postgres kafka kafka-init streaming-db-init streaming-consumer
```

Topic yang digunakan:

```text
Topic              : metropolia.events
Partition          : 1
Replication factor : 1
```

### Producer manual

Traffic:

```cmd
docker compose run --rm streaming-producer traffic --zone Z01 --volume 5200 --temperature 31 --rain 0 --weather Clear
```

Kualitas udara:

```cmd
docker compose run --rm streaming-producer air-quality --zone Z01 --pm25 85 --pm10 120 --no2 42 --co 900 --o3 30 --temperature 31 --rain 0 --wind-speed 2.5
```

Kecelakaan:

```cmd
docker compose run --rm streaming-producer accident --zone Z01 --severity serious --casualties 2 --vehicles 2
```

Input harus dikirim melalui producer. Melakukan `INSERT` langsung melalui
DBeaver melewati Kafka dan Spark, sehingga tidak menunjukkan ETL streaming.

### Producer otomatis

```cmd
docker compose --profile auto up -d --build streaming-producer-auto
```

Secara default producer otomatis:

- mengirim event traffic dan kualitas udara setiap lima detik;
- merotasi zona Z01 sampai Z04;
- mengirim event kecelakaan pada siklus pertama dan setiap 13 siklus;
- menggunakan waktu Asia/Jakarta;
- membuat UUID baru sebagai `event_id`.

Pantau producer:

```cmd
docker compose --profile auto logs -f streaming-producer-auto
```

Pantau consumer:

```cmd
docker compose logs -f streaming-consumer
```

Hentikan producer otomatis:

```cmd
docker compose stop streaming-producer-auto
```

Instruksi streaming yang lebih lengkap tersedia di `STREAMING_DEMO.md`.

## Pemeriksaan hasil streaming

Event yang berhasil diproses:

```sql
SELECT
    event_id,
    event_type,
    event_hour,
    zone_id,
    emitted_at,
    processed_at,
    processed_at - emitted_at AS processing_latency
FROM etl_stream_event_log
ORDER BY processed_at DESC
LIMIT 20;
```

Ringkasan micro-batch:

```sql
SELECT *
FROM etl_stream_batch_log
ORDER BY processed_at DESC
LIMIT 20;
```

Hasil pada mart:

```sql
SELECT
    event_hour,
    zone_id,
    traffic_volume,
    average_speed,
    congestion_level,
    pm25,
    pm10,
    accident_count,
    casualty_count,
    refreshed_at
FROM mart_metropolia_hourly
ORDER BY refreshed_at DESC
LIMIT 20;
```

## Modeling

Data untuk modeling tersedia melalui PostgreSQL view:

```text
model_training_dataset
```

Pembagian data yang digunakan:

- Training: 2022 sampai 2023.
- Validation: 2024.
- Test: 1 Januari sampai 31 Agustus 2025.
- Operasional: daily batch dan streaming.

Pembagian dilakukan berdasarkan waktu, bukan secara acak.

## Menghentikan layanan

Hentikan container tanpa menghapus data:

```cmd
docker compose stop
```

Nyalakan kembali:

```cmd
docker compose start
```

Jangan menggunakan perintah berikut jika data masih dibutuhkan:

```cmd
docker compose down -v
```

Opsi `-v` menghapus volume PostgreSQL, metadata Airflow, data Kafka, dan
checkpoint streaming.

## Troubleshooting

### Port 5432 atau 8080 sudah digunakan

Periksa container yang sedang berjalan:

```cmd
docker ps
```

Hentikan container lain yang menggunakan port yang sama atau ubah port host
pada `docker-compose.yml`.

### Log Airflow menghasilkan 403

Pastikan semua komponen Airflow menggunakan nilai
`AIRFLOW__WEBSERVER__SECRET_KEY` yang sama. Setelah mengubah nilai tersebut,
buat ulang container:

```cmd
docker compose down
docker compose up -d
```

Jangan menambahkan `-v`.

### Consumer streaming menggunakan terlalu banyak RAM

Ubah `.env`:

```env
STREAM_SPARK_DRIVER_MEMORY=512m
STREAM_SPARK_EXECUTOR_MEMORY=512m
```

Jangan menjalankan historical backfill dan Spark Structured Streaming secara
bersamaan pada komputer dengan RAM terbatas.

## Batasan simulasi

- Kota, zona, dan koordinat Subaraya merupakan lokasi sintetis.
- Data traffic dari satu sensor direplikasi ke empat zona menggunakan faktor tetap.
- Dua belas stasiun kualitas udara dipetakan ke empat zona.
- Data kecelakaan dipetakan secara deterministik menggunakan hash ID.
- Producer otomatis menghasilkan event simulasi baru.
- Konfigurasi Kafka menggunakan satu broker, satu topic, satu partition, dan
  replication factor satu.
- Hubungan antarsumber digunakan sebagai skenario simulasi dan tidak menunjukkan
  hubungan kausal pada kota nyata.
